import datetime
import json
import os
import time
import warnings

import boto3
import gspread
import polars as pl
import polars.selectors as cs
import pytz
import requests
import s3fs
from Common.cast_frame import add_computed_cols, cast_frame
from Common.common import WRITE_DATA_BUCKET, get_df, get_secret
from Common.gamesettings import (
    gamesetting_equal_columns,
    gamesettings,
    gamesettings_scav,
    higher_harder,
    lower_harder,
    nutty_b_main,
    nuttyb_hp_multiplier,
    possible_tweak_columns,
    various,
)
from Common.logger import get_logger

logger = get_logger()
dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401


replays_root_file_name = 'replays.parquet'
replay_details_file_name = 'replays_gamesettings.parquet'
web_bucket_path = 's3://pve-rating-web/'


def store_df(df, path, store_web=True):
    if dev:
        logger.info(f'writing {len(df)} to {path}')
        df.write_parquet(path)
    else:
        parquet_path = os.path.join(WRITE_DATA_BUCKET, path)
        web_path = os.path.join(web_bucket_path, path) if store_web else ''
        logger.info(f'writing {len(df)} to {parquet_path} and {web_path}')
        fs = s3fs.S3FileSystem()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)

            with fs.open(parquet_path, mode='wb') as f:
                df.write_parquet(f)
            if store_web:
                with fs.open(web_path, mode='wb') as f:
                    df.write_parquet(f)


def main():
    games = get_df(replays_root_file_name)

    n_received_rows = page_size = int(os.environ.get('LIMIT', 2 if dev else 400))
    page = 1
    n_total_received_rows = 0
    while n_received_rows > 1 and page_size > 0 and page <= (1 if dev else 100):
        apiUrl = f'https://api.bar-rts.com/replays?limit={page_size}&preset=team&hasBots=true&page={page}'
        if page > 1:
            time.sleep(0.4)
        logger.info(f'fetching {apiUrl}')
        replays_json = requests.get(
            apiUrl,
            headers={'User-Agent': os.environ['DISCORD_USERNAME']},
        ).json()

        data = replays_json['data']

        api = (
            pl.DataFrame(data)
            .drop('Map')
            .filter(~pl.col('id').is_in(games['id'].to_list()))
        )

        n_received_rows = len(api)
        n_total_received_rows += n_received_rows
        n_before_games = len(games)
        games = pl.concat(
            [
                games,
                api.with_columns(
                    startTime=pl.col('startTime').str.to_datetime('%+', time_unit='ns')
                ).select(['startTime', 'durationMs', 'AllyTeams', 'id']),
            ],
            how='vertical_relaxed',
        )
        logger.info(f'games {n_before_games} + {n_received_rows} = {len(games)}')
        page += 1
    games.rechunk()
    del api

    if n_total_received_rows > 0:
        store_df(games, replays_root_file_name)

    games = add_computed_cols(games).rename({'AllyTeams': 'AllyTeamsList'})

    logger.info('Fetching replay details')
    replay_details_cache = get_df(replay_details_file_name)

    games = games.join(
        replay_details_cache, how='left', on='id', validate='1:1', coalesce=True
    ).drop(cs.ends_with('_right'))
    del replay_details_cache

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
    logger.info(f'previousPlayerWinStartTime: {previousPlayerWinStartTime}')

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

    # fetch new
    unfetched = (
        games.filter(pl.col('fetch_success').is_null())
        .sort(by='startTime', descending=True)
        .select('id')
    )
    to_fetch_ids = unfetched[
        : 2 if dev else (int(os.environ.get('details_fetch_limit')))
    ]
    if len(to_fetch_ids) == 0:
        logger.info('no new games to fetch')
        if not dev:
            return
    else:
        logger.info(f'fetching {len(to_fetch_ids)} of {len(unfetched)} missing games')

        fetched = []
        for index, replay_id in enumerate(to_fetch_ids.iter_rows()):
            logger.info(f'fetching {index+1}/{len(to_fetch_ids)} {replay_id[0]}')
            fetched.append(api_replay_detail(replay_id[0]))

        games = games.select(
            games.columns
            + [
                pl.lit(None).alias(x)
                for x in set(fetched[0].keys()) - set(games.columns)
            ]
        ).update(
            cast_frame(pl.DataFrame(fetched, strict=False)).drop('startTime'),
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
    store_df(games, replay_details_file_name)

    if not dev:
        logger.info('Invoking PveRating')
        lambda_client = boto3.client('lambda')

        lambda_client.invoke(
            FunctionName='PveRating',
            InvocationType='Event',
        )

    return 'done fetching'

    games = games.filter(
        pl.col('raptors_win').eq(False)
        & pl.col('scavengers_win').eq(False)
        & pl.col('barbarian_win').eq(False)
    )

    # stop without any new wins
    newMaxStartTime, newMaxEndTime = (
        games.filter(
            ('raptors' & pl.col('raptors_win').eq(False))
            | ('scavengers' & pl.col('scavengers_win').eq(False))
            | ('barbarian' & pl.col('barbarian_win').eq(False))
        )
        .select(
            pl.col('startTime'),
            (pl.col('startTime') + pl.duration(milliseconds='durationMs'))
            .cast(pl.Datetime)
            .alias('newEndTime'),
        )
        .max()
    )
    tz_stockholm = pytz.timezone('Europe/Stockholm')
    previousStockholm = previousPlayerWinStartTime.replace(tzinfo=pytz.utc).astimezone(
        tz_stockholm
    )

    # FIXME previousPlayerWinStartTime
    if (
        previousPlayerWinStartTime
        and newMaxStartTime.item() <= previousPlayerWinStartTime
    ):
        logger.info(
            f'no new wins since {previousStockholm} ({newMaxStartTime.item()} <= {previousPlayerWinStartTime})'
        )
        # if not dev:
        #     return
    else:
        logger.info(
            f'new wins since {previousStockholm}: {newMaxStartTime.item().replace(tzinfo=pytz.utc).astimezone(tz_stockholm)}'
        )

    all_nuttyb_tweaks = (
        nutty_b_main
        + [string for x in nuttyb_hp_multiplier.values() for string in x]
        + various
    )
    any_nuttyb_tweaks_or_empty = all_nuttyb_tweaks + ['']

    def gamesettings_mode(row):
        if row['raptors']:
            ai_gamesettings = gamesettings
            ai_start_setting_name = 'raptor_raptorstart'
        else:
            ai_gamesettings = gamesettings_scav
            ai_start_setting_name = 'scav_scavstart'

        for mode_name, settings in ai_gamesettings.items():
            match = False
            for setting, value in settings.items():
                if row[setting] is not None and (
                    row[setting] == value
                    or (setting in higher_harder and row[setting] > value)
                    or (setting in lower_harder and row[setting] < value)
                    or (setting == ai_start_setting_name and row[setting] == 'avoid')
                ):
                    # logger.info(f'value matching mode {mode_name} {setting} {row[setting]} ~= {value} higher harder {setting in higher_harder} lower harder {setting in lower_harder}')
                    match = True
                elif row[setting] != value:
                    # troubleshoot debug replay_id
                    # if (
                    #     row['id'] == 'e3842966b8373248b883e4dee6269114'
                    #     # and mode_name == 'Rush'
                    # ):
                    #     logger.info(
                    #         f'value not matching mode {mode_name} {setting} {row[setting]} != {value}'
                    #     )
                    #     s()
                    match = False
                    break
                else:
                    raise ValueError(f'unhandled setting {setting} value {value}')
            if match:
                return mode_name
        return ''

    gamesettings_mode_enum = pl.Enum(
        [
            'Gauntlet',
            '0 grace zerg',
            'Zerg',
            '0 grace',
            'Rush',
            'Gauntlet 1.48',
            'Zerg 1.48',
            'Rush 1.48',
            'Max spawn, 1k res',
            'Max spawn, 5k res',
            'Max spawn, 10k res',
            'Max spawn, 1k res, extraunits',
            'Max spawn, 5k res, extraunits',
            'Max spawn, 10k res, extraunits',
            '',
        ]
    )

    games = games.with_columns(
        pl.Series(
            'Gamesettings Mode',
            [gamesettings_mode(x) for x in games.iter_rows(named=True)],
            dtype=gamesettings_mode_enum,
        ),
    )

    def format_regular_diff(string):
        if string is None:
            return '"Missing Details"'
        return string.replace('very', 'very ').title()

    nuttyb_exclusive_modes = {
        'Gauntlet',
        '0 grace zerg',
        'Zerg',
        '0 grace',
        'Rush',
        'Gauntlet 1.48',
        'Zerg 1.48',
        'Rush 1.48',
    }

    def Difficulty(row):
        # if row['id'] == 'e3842966b8373248b883e4dee6269114':
        #     s()
        if (
            row['nuttyb_tweaks_exclusive']
            and row['Gamesettings Mode'] in nuttyb_exclusive_modes
        ):
            if row['nuttyb_hp']:
                return f"NuttyB Default {row['nuttyb_hp']}"
            elif row['raptors']:
                return f'NuttyB Default Regular HP ({format_regular_diff(row['raptor_difficulty'])})'
            elif row['scavengers']:
                return f'NuttyB Default Regular HP ({format_regular_diff(row['scav_difficulty'])})'
        elif row['nuttyb_main']:
            if row['nuttyb_hp']:
                return f"NuttyB Main & HP {row['nuttyb_hp']}"
            elif row['raptor_difficulty']:
                return f"NuttyB Main & {format_regular_diff(row['raptor_difficulty'])}"
            else:
                return 'NuttyB Main "Missing Details"'

        elif row['nuttyb_hp']:
            return f'NuttyB HP {row["nuttyb_hp"]}'
        elif row['raptors'] and not row['raptors_win'] and row['raptor_difficulty']:
            return f'Raptors {format_regular_diff(row["raptor_difficulty"])}'
        elif row['scavengers'] and not row['raptors_win'] and row['scav_difficulty']:
            return f'Scavengers {format_regular_diff(row["scav_difficulty"])}'
        elif (row['raptors'] or row['scavengers'] or row['barbarian']) and (
            row['raptors_win'] or row['scavengers_win'] or row['barbarian_win']
        ):
            return 'Mixed AIs'

        if not dev and row['barbarian'] == False:
            _dict = {
                key: value
                for key, value in row.items()
                if key
                in [
                    'id',
                    'startTime',
                    'nuttyb_main',
                    'nuttyb_hp',
                    'nuttyb_tweaks_exclusive',
                    'raptor_difficulty',
                    'scav_difficulty',
                    'raptors',
                    'scavengers',
                ]
            }
            logger.debug(
                f'no diff found for {_dict}',
            )
        return ''

    games = games.with_columns(
        nuttyb_main=pl.concat_list(possible_tweak_columns)
        .list.set_intersection(nutty_b_main)
        .list.len()
        > 0,
        nuttyb_hp=pl.when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(nuttyb_hp_multiplier['Epicest'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epicest'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(nuttyb_hp_multiplier['Epicer+'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epicer+'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(nuttyb_hp_multiplier['Epic++'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epic++'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(nuttyb_hp_multiplier['Epic+'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epic+'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(nuttyb_hp_multiplier['Epic'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epic')),
        nuttyb_tweaks_exclusive=(
            pl.concat_list(possible_tweak_columns)
            .list.set_difference(any_nuttyb_tweaks_or_empty)
            .list.len()
            == 0
        )
        & (
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(all_nuttyb_tweaks)
            .list.len()
            > 0
        ),
        nuttyb_tweaks_inclusive=(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(all_nuttyb_tweaks)
            .list.len()
            > 0
        ),
    )

    # troubleshoot debug default
    # extra_tweaks = (
    #     games.filter(pl.col('id') == 'e3842966b8373248b883e4dee6269114')
    #     .select(
    #         pl.concat_list(possible_tweak_columns)
    #         .list.set_difference(any_nuttyb_tweaks_or_empty)
    #         .alias('extra tweaks')
    #     )
    #     .to_series()
    #     .to_list()[0]
    # )
    # if len(extra_tweaks) > 0:
    #     print(f'len extra tweaks {len(extra_tweaks)}')

    #     import subprocess

    #     cmd = f'echo "{extra_tweaks[0].strip()}" | /mnt/c/Windows/System32/clip.exe'
    #     subprocess.check_call(cmd, shell=True)
    #     s()
    # else:
    #     print('no extra tweaks')

    difficulty_enum = pl.Enum(
        [
            'NuttyB Default Epicest',
            'NuttyB Default Epicer+',
            'NuttyB Default Epic++',
            'NuttyB Default Epic+',
            'NuttyB Default Epic',
            'NuttyB Default Regular HP (Epic)',
            'NuttyB Default Regular HP (Very Hard)',
            'NuttyB Default Regular HP (Hard)',
            'NuttyB Default Regular HP (Normal)',
            'NuttyB Default Regular HP (Easy)',
            'NuttyB Default Regular HP (Very Easy)',
            'NuttyB Main & HP Epicest',
            'NuttyB Main & HP Epicer+',
            'NuttyB Main & HP Epic++',
            'NuttyB Main & HP Epic+',
            'NuttyB Main & HP Epic',
            'NuttyB HP Epicest',
            'NuttyB HP Epicer+',
            'NuttyB HP Epic++',
            'NuttyB HP Epic+',
            'NuttyB HP Epic',
            'NuttyB Main & Epic',
            'NuttyB Main & Very Hard',
            'NuttyB Main & Hard',
            'NuttyB Main & Normal',
            'NuttyB Main & Easy',
            'NuttyB Main & Very Easy',
            'NuttyB Main "Missing Details"',
            'Raptors Epic',
            'Raptors Very Hard',
            'Raptors Hard',
            'Raptors Normal',
            'Raptors Easy',
            'Raptors Very Easy',
            'Raptors "Missing Details"',
            'Scavengers Epic',
            'Scavengers Very Hard',
            'Scavengers Hard',
            'Scavengers Normal',
            'Scavengers Easy',
            'Scavengers Very Easy',
            'Scavengers "Missing Details"',
            'Mixed AIs',
            '',
        ],
    )

    games = games.with_columns(
        pl.Series(
            'Difficulty',
            [Difficulty(x) for x in games.iter_rows(named=True)],
            dtype=difficulty_enum,
        ),
    )

    def awards(row):
        if not row['AllyTeams']:
            return pl.Series([0, 0])

        player_team_id = None
        for ally_team in row['AllyTeams']:
            if len(ally_team['Players']) > 0:
                for player in ally_team['Players']:
                    if player['name'] == row['Player'] and 'teamId' in player:
                        player_team_id = player['teamId']
                        break

        if player_team_id is None or not row['awards']:
            return pl.Series([0, 0])

        damage = 0
        eco = 0
        try:
            if row['awards']['fightingUnitsDestroyed'] and (
                player_team_id == row['awards']['fightingUnitsDestroyed'][0]['teamId']
            ):
                damage = 1
        except (KeyError, TypeError):
            damage = 0

        try:
            if player_team_id == row['awards']['mostResourcesProduced']['teamId']:
                eco = 1
        except KeyError:
            eco = 0

        return pl.Series([damage, eco])

    players = (
        games.explode('players')
        .rename({'players': 'Player'})
        .filter(pl.col('winners').list.contains(pl.col('Player')))
    )
    del games

    damage_awards, eco_awards = zip(*[awards(x) for x in players.iter_rows(named=True)])

    players = players.with_columns(
        pl.Series(
            '🏆DMG',
            damage_awards,
        ),
        pl.Series(
            '🏆ECO',
            eco_awards,
        ),
    ).sort(['Difficulty', 'Gamesettings Mode'])

    if dev:
        gc = gspread.service_account()
        spreadsheet = gc.open_by_key('1w8Ng9GGUo6DU0rBFRYRnC_JxK8nHTSSjf1Nbq1e-3bc')
    else:
        try:
            gc = gspread.service_account_from_dict(json.loads(get_secret()))
            spreadsheet = gc.open_by_key('1oI7EJIUiwLLXDMBgky2BN8gM6eaQRb9poWGiP2IKot0')
        except gspread.exceptions.APIError as e:
            logger.exception(e)
            logger.info('failed connection to google, stopping')
            return 'failed'

    update_sheet(spreadsheet, grouped(players), ' All', newMaxEndTime)
    players = players.filter(pl.col('Difficulty') != 'Mixed AIs')
    update_sheet(
        spreadsheet,
        grouped(players.filter(pl.col('scavengers') == True)),
        ' Scavengers',
        newMaxEndTime,
    )
    update_sheet(
        spreadsheet,
        grouped(
            players.filter(
                (pl.col('raptors') == True)
                & (pl.col('nuttyb_tweaks_inclusive') == False)
            ),
        ),
        ' Raptors Regular',
        newMaxEndTime,
    )
    update_sheet(
        spreadsheet,
        grouped(players.filter(pl.col('nuttyb_tweaks_inclusive') == True)),
        ' NuttyB',
        newMaxEndTime,
    )

    return 'done'


def grouped(to_group_df):
    if len(to_group_df) == 0:
        return to_group_df
    all_groups = []
    logger.info(f'grouping {len(to_group_df)} rows')

    for index, ((group_diff, group_mode), group_df) in enumerate(
        to_group_df.group_by(
            ['Difficulty', 'Gamesettings Mode'],
            maintain_order=True,
        )
    ):
        group_name = str(group_diff) + (
            ' - ' + group_mode if group_mode and group_mode is not None else ''
        )

        group_players = (
            group_df.group_by(['Player'])
            .agg(
                pl.col('Player').count().alias('n_victories'),
                (pl.sum('🏆DMG') + pl.sum('🏆ECO')).alias('awards_sum'),
                pl.sum('🏆DMG').str.replace(r'^0$', ''),
                pl.sum('🏆ECO').str.replace(r'^0$', ''),
                pl.col('id')
                .sort_by('startTime', descending=False)
                .map_batches(
                    lambda replay_ids: links_cell(
                        [
                            (f'https://bar-rts.com/replays/{replay_id}', index)
                            for index, replay_id in list(
                                reversed(list(enumerate(replay_ids)))
                            )
                        ]
                    ),
                )
                .first()
                .alias('Victories'),
            )
            .with_columns(
                pl.col('Player').str.to_lowercase().alias('player'),
            )
            .sort(
                [
                    'n_victories',
                    'awards_sum',
                    '🏆DMG',
                    'player',
                ],
                descending=[
                    True,
                    True,
                    True,
                    False,
                ],
            )
            .select(
                [
                    'Player',
                    'Victories',
                    '🏆DMG',
                    '🏆ECO',
                ]
            )
        )

        all_groups.append(
            [
                group_name,
                group_players
                if index == 0
                else group_players.select(pl.all().name.map(lambda x: f'{x}_{index}')),
            ]
        )

    return all_groups


def update_sheet(spreadsheet, groups, sheet_name_postfix, last_win):
    if len(groups) == 0:
        return

    top_header, data_groups = zip(*groups)

    second_header = data_groups[0].columns * len(data_groups)

    top_header = list(top_header)
    top_header_expanded = [''] * int(3 / 4 * len(second_header))
    for i in range(0, len(second_header)):
        if i % 4 == 0:
            top_header_expanded.insert(i, top_header.pop(0))

    data_values = pl.concat(data_groups, how='horizontal').rows()
    values = [top_header_expanded] + [second_header] + data_values

    n_rows = len(data_values) + 2
    len_second_header = len(second_header)

    sheet_name = datetime.date.today().strftime('%Y-%m') + sheet_name_postfix
    logger.info(
        f"updating sheet '{sheet_name}', {n_rows} rows, {len_second_header} cols"
    )
    new_sheet = False
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=n_rows,
            cols=len_second_header,
            index=0,
        )
        new_sheet = True

    worksheet.update(
        values=values,
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    unicode_char_columns = [index for index, x in enumerate(second_header) if '🏆' in x]
    player_columns = [index for index, x in enumerate(second_header) if 'Player' in x]

    sheet_id = worksheet._properties['sheetId']
    spreadsheetTitle = (
        f'Raptor Leaderboard & Stats, Last Win: {last_win.item():%Y-%m-%d %H:%M} UTC'
        + (' - dev test' if dev else '')
    )
    body = {
        'requests': (
            (
                [
                    {
                        'updateSheetProperties': {
                            'properties': {
                                'sheetId': sheet_id,
                                'gridProperties': {'frozenRowCount': 2},
                            },
                            'fields': 'gridProperties(frozenRowCount)',
                        }
                    },
                    {
                        'addBanding': {
                            'bandedRange': {
                                'bandedRangeId': sheet_id,
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': 2,
                                },
                                'rowProperties': {
                                    'firstBandColorStyle': {
                                        'rgbColor': {
                                            'red': 243 / 255,
                                            'green': 243 / 255,
                                            'blue': 243 / 255,
                                        },
                                    },
                                    'secondBandColorStyle': {
                                        'themeColor': 'BACKGROUND'
                                    },
                                },
                            },
                        },
                    },
                ]
                if new_sheet
                else []
            )
            + [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'fontSize': 11,
                                },
                                'hyperlinkDisplayType': 'LINKED',
                            }
                        },
                        'fields': 'userEnteredFormat',
                    }
                },
                {
                    'autoResizeDimensions': {
                        'dimensions': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                        }
                    }
                },
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'fontSize': 10,
                                },
                                'hyperlinkDisplayType': 'LINKED',
                            }
                        },
                        'fields': 'userEnteredFormat',
                    }
                },
                {
                    'updateSpreadsheetProperties': {
                        'properties': {'title': spreadsheetTitle},
                        'fields': 'title',
                    }
                },
            ]
            + [
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': index,
                            'endIndex': index + 1,
                        },
                        'properties': {'pixelSize': 52},
                        'fields': 'pixelSize',
                    }
                }
                for index in unicode_char_columns
            ]
            + [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startColumnIndex': index,
                            'endColumnIndex': index + 1,
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'LEFT',
                            }
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment)',
                    }
                }
                for index in player_columns
            ]
        )
    }
    spreadsheet.batch_update(body)


if __name__ == '__main__':
    main()
