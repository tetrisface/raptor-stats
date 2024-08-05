import os

import polars as pl
import polars.selectors as cs

from types import SimpleNamespace
from common.cast_frame import (
    add_computed_cols,
    cast_frame,
    map_replace_regex_string,
    reorder_column,
)
from common.logger import get_logger, lambda_handler_decorator
from common.common import (
    FILE_SERVE_BUCKET,
    READ_DATA_BUCKET,
    replay_details_file_name,
    s3_download_df,
    s3_upload_df,
    user_ids_name_map,
)

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401
logger = get_logger()


@lambda_handler_decorator
def main(*args):
    games = s3_download_df(READ_DATA_BUCKET, replay_details_file_name)
    games = add_computed_cols(cast_frame(games))

    grouped = pl.concat(
        [
            s3_download_df(
                FILE_SERVE_BUCKET, f'{prefix}.all.grouped_gamesettings.parquet'
            )
            for prefix in ['Barbarian', 'Raptors', 'Scavengers']
        ],
        how='diagonal',
    )

    gamesetting_games = (
        games.rename({'players': 'Players'})
        .with_columns(
            pl.when('raptors')
            .then(pl.lit('Raptors'))
            .when('scavengers')
            .then(pl.lit('Scavengers'))
            .when('barbarian')
            .then(pl.lit('Barbarian'))
            .alias('AI'),
            pl.when(
                (
                    pl.col('raptors_win')
                    | pl.col('scavengers_win')
                    | pl.col('barbarian_win')
                )
            )
            .then(pl.lit('Loss'))
            .when('draw')
            .then(pl.lit('Draw'))
            .otherwise(pl.lit('Win'))
            .alias('Result'),
            pl.col('Players')
            .list.eval(pl.element().replace_strict(user_ids_name_map(games)))
            .list.join(', '),
            pl.col('Map')
            .struct.field('scriptName')
            .str.replace(
                map_replace_regex_string,
                '',
            )
            .alias('Map'),
        )
        .filter(pl.col('AI').is_not_null())[
            'startTime',
            'AI',
            'id',
            'Result',
            'Players',
            'Map',
            'Barbarian Handicap',
            'Barbarian Per Player',
        ]
        .join(
            grouped.with_columns(
                pl.col('Win Replays').list.set_union('Loss Replays').alias('id')
            )
            .drop('Winners', cs.matches('Replays'))
            .explode('id'),
            on='id',
            how='left',
            coalesce=True,
        )
        .drop(cs.ends_with('_right'))
        .sort('startTime', descending=True)
        .rename({'startTime': 'Start Time', 'id': 'Replay ID'})
    )

    gamesetting_games = reorder_column(gamesetting_games, 11, 'Barbarian Handicap')
    gamesetting_games = reorder_column(gamesetting_games, 11, 'Barbarian Per Player')
    s3_upload_df(
        gamesetting_games.head(10000),
        FILE_SERVE_BUCKET,
        'gamesetting_games.parquet',
    )


if __name__ == '__main__':
    main({}, SimpleNamespace(function_name='RecentGames'))
