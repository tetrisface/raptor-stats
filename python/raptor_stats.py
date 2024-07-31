import datetime
import os
import time
from types import SimpleNamespace

import polars as pl
import polars.selectors as cs
import requests
from common.cast_frame import add_computed_cols, cast_frame
from common.common import (
    READ_DATA_BUCKET,
    WRITE_DATA_BUCKET,
    FILE_SERVE_BUCKET,
    invoke_lambda,
    s3_download_df,
    s3_upload_df,
)
from common.gamesettings import gamesetting_equal_columns
from common.logger import get_logger, lambda_handler_decorator

logger = get_logger()
dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

replays_root_file_name = 'replays.parquet'
replay_details_file_name = 'replays_gamesettings.parquet'


@lambda_handler_decorator
def main(*args):
    games = s3_download_df(READ_DATA_BUCKET, replays_root_file_name)
    api = None

    n_received_rows = page_size = int(
        os.environ.get('LIST_PAGE_SIZE', 10 if dev else 100)
    )
    page = int(os.environ.get('LIST_PAGE_START', 1))
    n_total_received_rows = 0
    api_min_date_fetched = datetime.datetime.now(datetime.timezone.utc)
    update = bool(os.environ.get('LIST_PAGE_UPDATE', False))
    list_page_page_limit = int(os.environ.get('LIST_PAGE_PAGE_LIMIT', 1 if dev else 50))
    while (
        n_received_rows > 1
        and page_size > 0
        and page <= list_page_page_limit
        and api_min_date_fetched
        >= datetime.datetime.fromisoformat(
            os.environ.get('LIST_PAGE_DATE_LIMIT', '2024-04-01')
        ).replace(tzinfo=datetime.timezone.utc)
    ):
        apiUrl = f'https://api.bar-rts.com/replays?limit={page_size}&hasBots=true&page={page}'
        if page > 1:
            time.sleep(0.4)
        logger.info(
            f'fetching {apiUrl} received {n_received_rows}/{n_total_received_rows} page {page}/{list_page_page_limit} date {api_min_date_fetched}'
        )
        replays_json = requests.get(
            apiUrl,
            headers={'User-Agent': os.environ['DISCORD_USERNAME']},
        ).json()

        data = replays_json['data']

        api = (
            pl.DataFrame(data)
            .with_columns(
                pl.col('Map')
                .struct.field('scriptName')
                .str.replace(
                    r'(?i)[_\s]+[v\d\.]+\w*$',
                    '',
                )
                .alias('Map Name'),
                pl.col('startTime').str.to_datetime(
                    '%+', time_unit='ns', time_zone='UTC', strict=True, exact=True
                ),
            )
            .drop('Map')
            .filter(True if update else ~pl.col('id').is_in(games['id'].to_list()))
        )

        games = games.with_columns(
            [pl.lit(None).alias(x) for x in set(api.columns) - set(games.columns)]
        )

        n_received_rows = len(api)
        n_total_received_rows += n_received_rows
        n_before_games = len(games)
        api_min_date_fetched = api['startTime'].min()

        if api['startTime'].dtype != pl.Datetime:
            api = api.with_columns(
                startTime=pl.col('startTime').str.to_datetime('%+', time_unit='ns')
            )
        if update:
            games = games.update(
                api['id', 'Map Name'],
                how='left',
                on='id',
            )
        else:
            games = pl.concat(
                [
                    games,
                    api['startTime', 'durationMs', 'AllyTeams', 'id', 'Map Name'],
                ],
                how='vertical_relaxed',
            )
        logger.info(f'Games {n_before_games} + {n_received_rows} = {len(games)}')
        page += 1
    games = games.cast({'durationMs': pl.UInt32}, strict=True)
    games.rechunk()
    del api

    if n_total_received_rows > 0:
        s3_upload_df(games, WRITE_DATA_BUCKET, replays_root_file_name)
        s3_upload_df(games, FILE_SERVE_BUCKET, replays_root_file_name)

    games = add_computed_cols(games).rename({'AllyTeams': 'AllyTeamsList'})

    logger.info('Fetching replay details')
    replay_details_cache = s3_download_df(READ_DATA_BUCKET, replay_details_file_name)

    games = (
        games.join(
            replay_details_cache, how='left', on='id', validate='1:1', coalesce=True
        )
        .drop(cs.ends_with('_right'))
        .update(replay_details_cache['id', 'Map Name'], how='left', on='id')
    )
    del replay_details_cache

    assert (
        games.select(cs.ends_with('_right')).columns == []
    ), f'Failed joining root with details df adding extra columns {games.select(cs.ends_with("_right")).columns}'

    previousPlayerWinStartTime = (
        games.filter(
            'barbarian' & pl.col('barbarian_win').eq(False)
            | ('raptors' & pl.col('raptors_win').eq(False))
            | ('scavengers' & pl.col('scavengers_win').eq(False))
        )
        .select(pl.col('startTime'))
        .max()
        .item()
    )
    logger.info(f'PreviousPlayerWinStartTime: {previousPlayerWinStartTime}')

    def api_replay_detail(_replay_id):
        replay_details = {}
        url = ''
        if not dev:
            time.sleep(0.4)
        if _replay_id is not None:
            url = f'https://api.bar-rts.com/replays/{_replay_id}'
            response = requests.get(
                url, headers={'User-Agent': os.environ['DISCORD_USERNAME']}
            )
            if response.status_code == 200:
                response_json = response.json()
                replay_details = response_json.get('gameSettings')
                replay_details['awards'] = response_json.get('awards')
                replay_details['AllyTeams'] = response_json.get('AllyTeams')
                replay_details['Map'] = response_json.get('Map')
                replay_details['startTime'] = response_json.get('startTime')
                replay_details['id'] = _replay_id
                replay_details['fetch_success'] = True
                return replay_details
            else:
                logger.info(f'Failed to fetch data from {url}')
                replay_details['fetch_success'] = False
        return replay_details

    before_null_awards = len(
        games.filter(
            pl.col('awards')
            .struct.field('fightingUnitsDestroyed')
            .list[0]
            .struct.field('value')
            .is_null()
        )
    )
    before_null_map = len(
        games.filter(pl.col('Map').struct.field('scriptName').is_null())
    )

    # fetch new
    unfetched = (
        games.filter(pl.col('fetch_success').is_null())
        .sort(by='startTime', descending=True)
        .select('id')
    )
    to_fetch_ids = unfetched[
        : 10 if dev else (int(os.environ.get('details_fetch_limit')))
    ]
    if len(to_fetch_ids) == 0:
        logger.info('No new games to fetch')
        if not dev:
            return
    else:
        logger.info(f'Fetching {len(to_fetch_ids)} of {len(unfetched)} missing games')

        fetched = []
        for index, replay_id in enumerate(to_fetch_ids.iter_rows()):
            logger.info(f'Fetching {index+1}/{len(to_fetch_ids)} {replay_id[0]}')
            fetched.append(api_replay_detail(replay_id[0]))

        null_columns = [
            pl.lit(None).alias(x) for x in set(fetched[0].keys()) - set(games.columns)
        ]

        logger.info(f'Setting null columns {null_columns}')
        update_df = cast_frame(pl.DataFrame(fetched, strict=False)).drop('startTime')

        # nested awards are somehow not updated
        games = (
            games.join(
                update_df['id', 'awards', 'Map'],
                how='left',
                on='id',
                validate='1:1',
                coalesce=True,
            )
            .with_columns(
                pl.when(
                    pl.col('awards')
                    .struct.field('fightingUnitsDestroyed')
                    .list[0]
                    .struct.field('value')
                    .is_null()
                    & pl.col('awards_right')
                    .struct.field('fightingUnitsDestroyed')
                    .list[0]
                    .struct.field('value')
                    .is_not_null()
                )
                .then(pl.col('awards_right'))
                .otherwise(pl.col('awards'))
                .alias('awards'),
                pl.when(
                    pl.col('Map').struct.field('scriptName').is_null()
                    & pl.col('Map_right').struct.field('scriptName').is_not_null()
                )
                .then(pl.col('Map_right'))
                .otherwise(pl.col('Map'))
                .alias('Map'),
            )
            .drop(cs.ends_with('_right'))
        )

        games = games.select(games.columns + null_columns).update(
            update_df.drop('awards', 'Map'),
            how='left',
            on='id',
        )

    del to_fetch_ids, unfetched

    if not games.filter(pl.col('fetch_success') == False).is_empty():
        logger.info(
            f'failed to fetch {len(games.filter(pl.col('fetch_success') == False))} games'
        )

    null_columns_df = (
        games[list(gamesetting_equal_columns - {'nuttyb_hp', 'multiplier_maxdamage'})]
        .null_count()
        .transpose(include_header=True, header_name='setting', column_names=['value'])
        .filter(pl.col('value') > 0)
    )
    if len(null_columns_df) > 0:
        logger.warning(f'found null columns {null_columns_df}')

    logger.info(
        f'Null awards {before_null_awards} -> {len(games.filter(pl.col('awards').struct.field('fightingUnitsDestroyed').list[0].struct.field('value').is_null()))}'
    )
    logger.info(
        f'Null Map {before_null_map} -> {len(games.filter(pl.col('Map').struct.field('scriptName').is_null()))}'
    )

    # refetch game details
    # games = games.update(
    #     games.filter(
    #         # found nulls refetch
    #         pl.any_horizontal(pl.col(null_columns_df['setting'].to_list()).is_null())
    #         # date refetch etc
    #         # (pl.col('startTime').cast(pl.Date) > datetime.date(2024, 4, 26))
    #         # & pl.col('evocom').is_null()
    #         # pl.col('Map').struct.field('scriptName').is_null()
    #     ).select('id', pl.lit(None).alias('fetch_success')),
    #     on='id',
    #     include_nulls=True,
    # )

    # store
    s3_upload_df(games, WRITE_DATA_BUCKET, replay_details_file_name)
    s3_upload_df(games, FILE_SERVE_BUCKET, replay_details_file_name)

    invoke_lambda('PveRating', {})

    return 'done fetching'


if __name__ == '__main__':
    main({}, SimpleNamespace(function_name='RaptorStats'))
