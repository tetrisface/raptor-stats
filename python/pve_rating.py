import datetime
import io
import os
import random
import re
from types import SimpleNamespace

import boto3
import gspread
import numpy as np
import orjson
import polars as pl
import polars.selectors as cs
from common.cast_frame import (
    add_computed_cols,
    cast_frame,
    reorder_column,
    reorder_tweaks,
)
from common.common import (
    FILE_SERVE_BUCKET,
    READ_DATA_BUCKET,
    WRITE_DATA_BUCKET,
    get_secret,
    invoke_lambda,
    replay_details_file_name,
    s3_download_df,
    s3_upload_df,
    user_ids_name_map,
)
from common.gamesettings import (
    gamesetting_equal_columns,
    higher_harder,
    lower_harder,
    possible_tweak_columns,
    barbarian_gamesetting_equal_columns,
)
from common.logger import get_logger, lambda_handler_decorator
from spreadsheet import (
    get_or_create_worksheet,
    number_to_column_letter,
)

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

# Interpolation with quadratic fit
lobby_size_teammates_completion_fit_input = [1, 2, 5, 16]
lobby_size_teammates_completion_fit_output = [1, 4, 11, 40]
A = np.array([[x**2, x, 1] for x in lobby_size_teammates_completion_fit_input])
b = np.array(lobby_size_teammates_completion_fit_output)
(
    lobby_size_teammates_coef_a,
    lobby_size_teammates_coef_b,
    lobby_size_teammates_coef_c,
) = np.linalg.lstsq(A, b, rcond=None)[0]


@lambda_handler_decorator
def main(*args):
    _games = add_computed_cols(
        cast_frame(s3_download_df(READ_DATA_BUCKET, replay_details_file_name))
    )

    json_data = {
        'pve_ratings': {
            'BarbarianAI': process_games(
                _games.filter('barbarian' & ~pl.col('raptors') & ~pl.col('scavengers')),
                'Barbarian',
            ),
            'RaptorsAI': process_games(
                _games.filter('raptors' & ~pl.col('scavengers') & ~pl.col('barbarian')),
                'Raptors',
            ),
            'ScavengersAI': process_games(
                _games.filter('scavengers' & ~pl.col('raptors') & ~pl.col('barbarian')),
                'Scavengers',
            ),
        }
    }

    s3 = boto3.client('s3')

    if FILE_SERVE_BUCKET:
        with io.BytesIO() as buffer:
            buffer.write(orjson.dumps(json_data))
            buffer.seek(0)
            s3.put_object(
                Bucket=FILE_SERVE_BUCKET,
                Key='pve_ratings.json',
                Body=buffer,
                StorageClass='INTELLIGENT_TIERING',
            )


def group_games_players(games):
    logger.info('Basic player aggregates')
    award_sum_expression = pl.when(pl.col('players_extended').list.len().gt(1)).then(
        pl.when(pl.col('Player').eq(pl.col('damage_award'))).then(1).otherwise(0)
        + pl.when(pl.col('Player').eq(pl.col('eco_award'))).then(1).otherwise(0)
    )

    return (
        games.unnest('damage_eco_award')
        .explode('players')
        .rename({'players': 'Player'})
        .group_by('Player')
        .agg(
            pl.len().alias('n_games'),
            pl.col('Player').is_in('winners').mean().alias('Win Rate'),
            pl.when(
                pl.col('Player').is_in('winners')
                & pl.col('players_extended').list.len().gt(1)
            )
            .then(award_sum_expression)
            .otherwise(None)
            .drop_nulls()
            .mean()
            .alias('Award Rate'),
            pl.when(
                pl.col('Player').is_in('winners')
                & pl.col('players_extended').list.len().gt(1)
            )
            .then(award_sum_expression * (pl.col('players_extended').list.len() - 1))
            .otherwise(None)
            .drop_nulls()
            .mean()
            .fill_null(0.0)
            .alias('Weighted Award Rate'),
        )
    )


def group_games_gamesettings(games, prefix):
    if prefix == 'Barbarian':
        ai_win_column = 'barbarian_win'
        ai_gamesetting_equal_columns = barbarian_gamesetting_equal_columns | set(
            possible_tweak_columns
        )
        ai_gamesetting_lower_harder = {
            'comrespawn',
            'disable_fogofwar',
        }
        ai_gamesetting_higher_harder = {'Barbarian Per Player', 'Barbarian Handicap'}
        games = games.filter(
            ~pl.col('Barbarian Per Player').is_infinite(),
            *[pl.col(col).eq(1) for col in games.columns if 'multiplier_' in col],
        )
    elif prefix == 'Raptors':
        ai_win_column = 'raptors_win'
        ai_gamesetting_equal_columns = {
            x for x in gamesetting_equal_columns if 'scav_' not in x
        }
        ai_gamesetting_lower_harder = {x for x in lower_harder if 'scav_' not in x}
        ai_gamesetting_higher_harder = {x for x in higher_harder if 'scav_' not in x}
    elif prefix == 'Scavengers':
        ai_win_column = 'scavengers_win'
        ai_gamesetting_equal_columns = {
            x for x in gamesetting_equal_columns if 'raptor_' not in x
        }
        ai_gamesetting_lower_harder = {x for x in lower_harder if 'raptor_' not in x}
        ai_gamesetting_higher_harder = {x for x in higher_harder if 'raptor_' not in x}

    remove_cols = [
        col
        for col in sorted(
            ai_gamesetting_equal_columns
            | ai_gamesetting_lower_harder
            | ai_gamesetting_higher_harder
        )
        if games[col].n_unique() == 1
    ]
    logger.info(f'Removing non-unique columns {remove_cols}')
    non_unique_gamesetting_values = {
        k: v
        for k, v in games[remove_cols].unique().to_dicts()[0].items()
        if not ('tweak' in k and v == '') and not ('multiplier_' in k and v == 1)
    }
    games = games.drop(remove_cols)

    col_set = set(games.columns)
    ai_gamesetting_equal_columns = ai_gamesetting_equal_columns & col_set
    ai_gamesetting_lower_harder = ai_gamesetting_lower_harder & col_set
    ai_gamesetting_higher_harder = ai_gamesetting_higher_harder & col_set

    ai_gamesetting_all_columns = sorted(
        ai_gamesetting_equal_columns
        | ai_gamesetting_lower_harder
        | ai_gamesetting_higher_harder
    )

    n_replays = len(games)
    logger.info(f'Processing {n_replays} {prefix} games')

    null_columns_df = (
        games[
            [
                x
                for x in set(ai_gamesetting_all_columns + ['Map']) - {'nuttyb_hp'}
                if x in games.columns
            ]
        ]
        .null_count()
        .transpose(include_header=True, header_name='setting', column_names=['value'])
        .filter(pl.col('value') > 0)
    )
    if len(null_columns_df) > 0:
        logger.warning(f'found null columns {null_columns_df}')

    logger.info(
        'Merging/extending players wins of harder games into easier games and losses of easier into harder games'
    )
    not_null_compare_merge_columns = (
        ai_gamesetting_equal_columns
        | ai_gamesetting_lower_harder
        | ai_gamesetting_higher_harder
        | {ai_win_column, 'Map Name'}
    ) - {'nuttyb_hp'}

    skipped = 0
    for game in games.iter_rows(named=True):
        if any(
            [v is None for k, v in game.items() if k in not_null_compare_merge_columns]
        ):
            skipped += 1
            logger.debug(f'Skipping null game {skipped} {game['id']}')
            continue

        win = game[ai_win_column] == False

        _ai_gamesetting_equal_columns = ai_gamesetting_equal_columns
        _ai_gamesetting_higher_harder = ai_gamesetting_higher_harder
        _ai_gamesetting_lower_harder = ai_gamesetting_lower_harder

        if game.get('evocom') == 0:
            _ai_gamesetting_equal_columns = {
                x for x in _ai_gamesetting_equal_columns if 'evocom' not in x
            }
            _ai_gamesetting_higher_harder = {
                x for x in _ai_gamesetting_higher_harder if 'evocom' not in x
            }
            _ai_gamesetting_lower_harder = {
                x
                for x in _ai_gamesetting_lower_harder
                if 'evocom' not in x or x == 'evocom'
            }
        if game.get('commanderbuildersenabled') == 'disabled':
            _ai_gamesetting_equal_columns = {
                x for x in _ai_gamesetting_equal_columns if 'commanderbuilders' not in x
            }
            _ai_gamesetting_higher_harder = {
                x for x in _ai_gamesetting_higher_harder if 'commanderbuilders' not in x
            }
            _ai_gamesetting_lower_harder = {
                x
                for x in _ai_gamesetting_lower_harder
                if 'commanderbuilders' not in x or x == 'commanderbuildersenabled'
            }
        if game.get('assistdronesenabled') == 'disabled':
            _ai_gamesetting_equal_columns = {
                x for x in _ai_gamesetting_equal_columns if 'assistdrones' not in x
            }
            _ai_gamesetting_higher_harder = {
                x for x in _ai_gamesetting_higher_harder if 'assistdrones' not in x
            }
            _ai_gamesetting_lower_harder = {
                x
                for x in _ai_gamesetting_lower_harder
                if 'assistdrones' not in x or x == 'assistdronesenabled'
            }

        if game.get('nuttyb_hp') is None:
            _ai_gamesetting_higher_harder -= {'nuttyb_hp'}

        merge_games = games.filter(
            pl.col('id').ne(game['id']),
            pl.col('nuttyb_hp').is_null() if game['nuttyb_hp'] is None else True,
            *[
                pl.col(x).eq(game[x])
                for x in (_ai_gamesetting_equal_columns | {'Map Name'})
            ],
            *[
                (pl.col(x).le(game[x]) if win else pl.col(x).ge(game[x]))
                for x in _ai_gamesetting_higher_harder
            ],
            *[
                (pl.col(x).ge(game[x]) if win else pl.col(x).le(game[x]))
                for x in _ai_gamesetting_lower_harder
            ],
        )

        if len(merge_games) == 0:
            continue

        games = games.update(
            merge_games.with_columns(
                pl.col(f'Merged {'Win' if win else 'Loss'} Replays')
                .list.concat(pl.lit([game['id']]))
                .alias(f'Merged {'Win' if win else 'Loss'} Replays'),
                winners_extended=pl.col('winners_extended').list.concat(
                    pl.lit(game['winners'] if win else [])
                ),
                players_extended=pl.col('players_extended').list.concat(
                    pl.lit(game['players'])
                ),
            ).select(
                'id',
                'winners_extended',
                'players_extended',
                'Merged Win Replays',
                'Merged Loss Replays',
            ),
            on='id',
        )

    games = games.cast(
        {'winners_extended': pl.List(pl.UInt32), 'players_extended': pl.List(pl.UInt32)}
    )

    logger.info('Grouping gamesettings')
    group_by_columns = ['Map Name'] + list(ai_gamesetting_all_columns)
    grouped_gamesettings = (
        games.unnest('damage_eco_award')
        .group_by(group_by_columns)
        .agg(
            (
                1
                - (
                    pl.col('winners_extended').flatten().drop_nulls().n_unique()
                    / pl.col('players_extended').flatten().drop_nulls().n_unique()
                )
            ).alias('Difficulty'),
            pl.col('winners_extended')
            .sort_by('damage_award_value', descending=True)
            .flatten()
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('winners'),
            pl.col('winners_extended')
            .flatten()
            .drop_nulls()
            .n_unique()
            .alias('#Winners'),
            pl.col('players_extended')
            .sort_by('damage_award_value', descending=True)
            .flatten()
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Players'),
            pl.col('players_extended')
            .flatten()
            .drop_nulls()
            .n_unique()
            .alias('#Players'),
            pl.when(pl.col(ai_win_column).eq(False))
            .then(pl.col('id'))
            .sort_by('damage_award_value', descending=True)
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Win Replays'),
            pl.col('Merged Win Replays')
            .sort_by('damage_award_value', descending=True)
            .flatten()
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Merged Win Replays'),
            pl.when(pl.col(ai_win_column).ne(False))
            .then(pl.col('id'))
            .sort_by('durationMs', descending=True)
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Loss Replays'),
            pl.col('Merged Loss Replays')
            .sort_by('durationMs', descending=True)
            .flatten()
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Merged Loss Replays'),
            pl.col('winners')
            .sort_by('damage_award_value', descending=True)
            .flatten()
            .drop_nulls()
            .unique()
            .alias('winners_flat'),
            pl.when(pl.col('winners').list.len() > 0)
            .then(pl.col('winners'))
            .sort_by('damage_award_value', descending=True)
            .drop_nulls()
            .alias('games_winners'),
        )
        .with_columns(
            pl.col('Merged Win Replays')
            .list.set_difference(pl.col('Win Replays'))
            .alias('Merged Win Replays'),
            pl.col('Merged Loss Replays')
            .list.set_difference(pl.col('Loss Replays'))
            .alias('Merged Loss Replays'),
        )
    )

    grouped_gamesettings = reorder_tweaks(grouped_gamesettings)
    return (
        grouped_gamesettings,
        ai_gamesetting_all_columns,
        non_unique_gamesetting_values,
    )


user_ids_names = {}


def process_games(games, prefix):
    global user_ids_names
    user_ids_names = user_ids_name_map(games)
    games = games.drop('AllyTeams', 'AllyTeamsList')
    games = games.with_columns(
        pl.lit([]).alias('Merged Win Replays'),
        pl.lit([]).alias('Merged Loss Replays'),
        winners=pl.col('winners').list.set_difference([prefix + 'AI']),
        players=pl.col('players').list.set_difference([prefix + 'AI']),
    ).with_columns(
        winners_extended=pl.col('winners'),
        players_extended=pl.col('players'),
    )
    basic_player_aggregates = group_games_players(games)

    (
        grouped_gamesettings_rating,
        ai_gamesetting_all_columns,
        non_unique_gamesetting_values,
    ) = group_games_gamesettings(games, prefix)
    del games

    logger.info('Creating pastes')
    pastes = []
    for index, row in enumerate(grouped_gamesettings_rating.iter_rows(named=True)):
        row = {**non_unique_gamesetting_values, **row}
        _str = (
            '\n!preset coop\n!draft_mode disabled\n!unit_market 1\n!teamsize 16\n'
            + (f'!map {row["Map Name"]}\n' if row['Map Name'] else '')
        )

        for key, value in row.items():
            value = str(round(value, 1) if '_spawntimemult' in key else value).strip()
            if (
                key in {'nuttyb_hp', 'Barbarian Handicap', 'Barbarian Per Player'}
                or key not in ai_gamesetting_all_columns
                or value is None
                or value == ''
            ):
                continue

            value = re.sub('\\.0\\s*$', '', value)

            if ('multiplier_' in key and value == '1') or (
                'unit_restrictions_' in key and value == '0'
            ):
                continue

            if 'tweak' in key:
                _str += f'!bSet {key} {value}\n'
            else:
                _str += f'!{key} {re.sub("\\.0\\s*$", "", str(value))}\n'

        sheet_id = (
            '1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE'
            if dev
            else '18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY'
        )
        nuttyb_link_str = (
            ' and https://docs.google.com/document/d/1ycQV-T__ilKeTKxbCyGjlTKw_6nmDSFdJo-kPmPrjIs'
            if 'nuttyb_hp' in row and row['nuttyb_hp'] is not None
            else ''
        )
        pastes.append(
            _str
            + f'$welcome-message Settings from http://docs.google.com/spreadsheets/d/{sheet_id}#gid=0&range=I{index+2}{nuttyb_link_str}\n'
            + (
                f'$rename [Modded] {prefix}\n'
                if any(
                    v is not None and v != '' for k, v in row.items() if 'tweak' in k
                )
                else ''
            )
        )

    grouped_gamesettings_rating = grouped_gamesettings_rating.rename(
        {'Map Name': 'Map', 'winners': 'Winners'}
    )

    grouped_gamesettings_export = grouped_gamesettings_rating.with_columns(
        pl.col('Winners')
        .list.eval(
            pl.element().replace_strict(user_ids_names),
        )
        .list.join(', '),
        pl.col('Players')
        .list.eval(pl.element().replace_strict(user_ids_names))
        .list.join(', '),
        pl.Series(pastes).alias('Copy Paste'),
    )[
        'Difficulty',
        '#Winners',
        '#Players',
        'Winners',
        'Players',
        'Win Replays',
        'Merged Win Replays',
        'Loss Replays',
        'Merged Loss Replays',
        'Copy Paste',
        'Map',
        *ai_gamesetting_all_columns,
    ].sort(by=['Difficulty', '#Players', 'Map'], descending=[True, True, False])
    del pastes

    s3_upload_df(
        grouped_gamesettings_export,
        FILE_SERVE_BUCKET,
        prefix + '.all.grouped_gamesettings.parquet',
    )
    if prefix == 'Scavengers':
        invoke_lambda('RecentGames')
    difficulty_max = grouped_gamesettings_export['Difficulty'].max()
    difficulty_min = grouped_gamesettings_export['Difficulty'].min()

    s3_upload_df(
        grouped_gamesettings_export.filter(
            pl.col('Difficulty').is_between(
                difficulty_min, difficulty_max, closed='none'
            )
        ),
        FILE_SERVE_BUCKET,
        prefix + '.regular.grouped_gamesettings.parquet',
    )
    s3_upload_df(
        grouped_gamesettings_export.filter(pl.col('Difficulty') == difficulty_max),
        FILE_SERVE_BUCKET,
        prefix + '.unbeaten.grouped_gamesettings.parquet',
    )
    s3_upload_df(
        grouped_gamesettings_export.filter(pl.col('Difficulty') == difficulty_min),
        FILE_SERVE_BUCKET,
        prefix + '.cheese.grouped_gamesettings.parquet',
    )

    logger.info('Creating diff_tiered_export_limited df')
    diff_tiered_export_limited = pl.concat(
        [
            grouped_gamesettings_export.filter(pl.col('Difficulty') == 1)
            .sort('#Players', descending=True)
            .limit(50),
            grouped_gamesettings_export.filter(
                pl.col('Difficulty').is_between(0, 1, closed='none')
            ),
            grouped_gamesettings_export.filter(pl.col('Difficulty') == difficulty_min)
            .sort('#Players', descending=True)
            .limit(10),
        ]
    )

    diff_tiered_export_limited = diff_tiered_export_limited.with_columns(
        (
            pl.lit('=hyperlink("https://bar-rts.com/replays/')
            + pl.col('Win Replays').list.first()
            + pl.lit('";"')
            + pl.col('Win Replays').list.join(', ')
            + pl.lit('")')
        ).alias('Win Replays'),
        (
            pl.lit('=hyperlink("https://bar-rts.com/replays/')
            + pl.col('Merged Win Replays').list.first()
            + pl.lit('";"')
            + pl.col('Merged Win Replays').list.join(', ')
            + pl.lit('")')
        ).alias('Merged Win Replays'),
        (
            pl.lit('=hyperlink("https://bar-rts.com/replays/')
            + pl.col('Loss Replays').list.first()
            + pl.lit('";"')
            + pl.col('Loss Replays').list.join(', ')
            + pl.lit('")')
        ).alias('Loss Replays'),
        (
            pl.lit('=hyperlink("https://bar-rts.com/replays/')
            + pl.col('Merged Loss Replays').list.first()
            + pl.lit('";"')
            + pl.col('Merged Loss Replays').list.join(', ')
            + pl.lit('")')
        ).alias('Merged Loss Replays'),
    ).fill_null(' ')

    push_gamesettings_sheet(diff_tiered_export_limited, prefix)
    del diff_tiered_export_limited

    logger.info('Creating pve ratings')

    grouped_gamesettings_rating = grouped_gamesettings_rating.sort(
        by='#Players', descending=True
    )
    logger.info(
        f'gamesettings team completions iteration gs {len(grouped_gamesettings_rating)} min players {grouped_gamesettings_rating["#Players"][-1]}'
    )
    logger.info('Adding teammates weighted success rate + explode winners')

    grouped_gamesettings_rating = (
        grouped_gamesettings_rating.with_columns(
            pl.struct('games_winners', 'Difficulty').alias('games_winners_diff'),
        )
        .explode('Winners')
        .rename({'Winners': 'Player'})
    )

    logger.info('Adding teammates completion float')

    def diff_comp(x):
        diff_completions = []
        diff_goals = []
        completions = []
        for player_gamesetting in x:
            winner_name = player_gamesetting['_player']
            gamesetting = player_gamesetting['']
            difficulty_goal = gamesetting['Difficulty']

            completion = 0.0
            total_teammates = set()
            for game_teammates in gamesetting['games_winners']:
                if completion >= 1.0:
                    break
                game_teammates = set(game_teammates)
                if winner_name not in game_teammates:
                    continue

                new_teammates = game_teammates - total_teammates - {winner_name}
                total_teammates |= new_teammates
                lobby_size = len(game_teammates)
                n_new_teammates_and_me = len(new_teammates) + 1
                if lobby_size == 1:
                    teammates_completion_addition = 1.0
                else:
                    teammates_completion_addition = difficulty_goal / (
                        max(
                            1,
                            lobby_size_teammates_coef_a * (lobby_size**2)
                            + lobby_size_teammates_coef_b * lobby_size
                            + lobby_size_teammates_coef_c,
                        )
                        / n_new_teammates_and_me
                    )

                completion = min(
                    1.0,
                    completion + teammates_completion_addition,
                )
            diff_completions.append(difficulty_goal * completion)
            diff_goals.append(difficulty_goal)
            completions.append(completion)

        index = np.argmax(diff_completions)
        return completions[index]

    logger.info('Grouping by player and aggregating')
    grouped_gamesettings_rating = grouped_gamesettings_rating.group_by('Player').agg(
        pl.col('Difficulty').max().alias('Difficulty Record'),
        pl.struct(
            pl.col('Player').alias('_player'), pl.col('games_winners_diff').explode()
        )
        .map_elements(diff_comp, return_dtype=pl.Float32)
        .alias('Difficulty Completion'),
        pl.when(pl.col('Player').is_in('winners_flat'))
        .then(pl.col('Players').list.set_difference(pl.col('winners_flat')))
        .otherwise(pl.lit([]))
        .flatten()
        .drop_nulls()
        .n_unique()
        .alias('Difficulty Losers Sum'),
        pl.when(pl.col('Player').is_in('winners_flat'))
        .then(1)
        .otherwise(0)
        .sum()
        .alias('#Settings'),
    )

    logger.info('Join basic aggregates')
    grouped_gamesettings_rating = (
        grouped_gamesettings_rating.join(
            basic_player_aggregates,
            on='Player',
            how='left',
            validate='1:m',
            coalesce=True,
        )
        .drop(cs.ends_with('_right'))
        .drop_nulls('Player')
        .with_columns(
            pl.when(pl.col('n_games') > 20)
            .then(pl.lit('>20'))
            .otherwise(pl.col('n_games').clip(0, 20))
            .alias('#Games'),
            pl.col('Weighted Award Rate').rank().alias('Weighted Award Rate Rank'),
            (pl.col('Difficulty Record') * pl.col('Difficulty Completion'))
            .rank()
            .alias('Difficulty Rank'),
            pl.col('Difficulty Losers Sum').rank().alias('Difficulty Losers Sum Rank'),
            pl.col('n_games').clip(0, 20).rank().alias('#Games Rank'),
            pl.col('#Settings').rank().alias('Setting Combinations Rank'),
            pl.col('Win Rate').rank().alias('Win Rate Rank'),
        )
        .drop('n_games')
        .with_columns(
            (
                pl.col('Weighted Award Rate Rank') * 1
                + pl.col('Difficulty Losers Sum Rank') * 0.4
                + pl.col('Difficulty Rank') * 0.15
                + pl.col('Setting Combinations Rank') * 0.01
                + pl.col('#Games Rank') * 0.5
                + pl.col('Win Rate Rank') * 0.005
            )
            .rank()
            .alias('Combined Rank'),
        )
        .with_columns(
            (
                (
                    (pl.col('Combined Rank') - pl.col('Combined Rank').min())
                    / (pl.col('Combined Rank').max() - pl.col('Combined Rank').min())
                )
                * (30 - 0)
            )
            .round(2)
            .alias('PVE Rating'),
            pl.col('Player').replace_strict(user_ids_names, return_dtype=pl.String),
        )
        .sort(by=['PVE Rating', 'Player'], descending=[True, False], nulls_last=True)
        .fill_null('')
    )

    grouped_gamesettings_rating = reorder_column(
        grouped_gamesettings_rating, 1, 'Award Rate'
    )
    grouped_gamesettings_rating = reorder_column(
        grouped_gamesettings_rating, 2, 'Weighted Award Rate'
    )
    grouped_gamesettings_rating = reorder_column(
        grouped_gamesettings_rating, 7, '#Games'
    )
    grouped_gamesettings_rating = reorder_column(
        grouped_gamesettings_rating, 8, 'Win Rate'
    )

    logger.info('Updating sheets grouped_gamesettings')
    update_sheets(grouped_gamesettings_rating, prefix=prefix)
    s3_upload_df(
        grouped_gamesettings_rating,
        FILE_SERVE_BUCKET,
        f'PveRating.{prefix}_gamesettings.parquet',
    )
    return {
        player: rating
        for player, rating in zip(
            *grouped_gamesettings_rating.select('Player', 'PVE Rating')
            .to_dict(as_series=False)
            .values()
        )
    }


def push_gamesettings_sheet(df, prefix):
    if dev:
        spreadsheet_id = '1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE'
        gc = gspread.service_account()
        spreadsheet = gc.open_by_key(spreadsheet_id)
    else:
        spreadsheet_id = '18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY'
        gc = gspread.service_account_from_dict(orjson.loads(get_secret()))
        spreadsheet = gc.open_by_key(spreadsheet_id)
    gamesettings_worksheet = get_or_create_worksheet(
        spreadsheet, prefix + ' Gamesettings'
    )
    batch_requests = (
        [
            {
                'repeatCell': {
                    'range': {
                        'sheetId': gamesettings_worksheet.id,
                        'startColumnIndex': index,
                        'endColumnIndex': index + 1,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '0.?;0.?;',  # https://developers.google.com/sheets/api/guides/formats#number_format_tokens
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat',
                }
            }
            for index in filter(
                None,
                [
                    df.get_column_index('raptor_spawntimemult')
                    or df.get_column_index('scav_spawntimemult'),
                    df.get_column_index('Barbarian Per Player'),
                ],
            )
        ]
        + [
            {
                'repeatCell': {
                    'range': {
                        'sheetId': gamesettings_worksheet.id,
                        'startColumnIndex': col_index,
                        'endColumnIndex': col_index + 1,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '#',
                            },
                            'horizontalAlignment': 'RIGHT',
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat',
                }
            }
            for col_index in filter(
                None,
                [
                    df.get_column_index(x)
                    for x in [
                        'evocom',
                        'experimentalextraunits',
                        'experimentallegionfaction',
                        'ruins_only_t1',
                        'startenergy',
                        'startenergystorage',
                        'startmetal',
                        'startmetalstorage',
                    ]
                    + [x for x in df.columns if 'unit_restrictions' in x]
                ],
            )
        ]
        + [
            {
                'repeatCell': {
                    'range': {
                        'sheetId': gamesettings_worksheet.id,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'fontSize': 11,
                            },
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.fontSize',
                }
            },
            {
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': gamesettings_worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': df.columns.index('Map'),
                        'endIndex': min(
                            [df.columns.index(x) for x in df.columns if 'tweak' in x]
                        ),
                    },
                }
            },
            {
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': gamesettings_worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': max(
                            [df.columns.index(x) for x in df.columns if 'tweak' in x]
                        )
                        + 1,
                    },
                }
            },
            {
                'repeatCell': {
                    'range': {
                        'sheetId': gamesettings_worksheet.id,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'fontSize': 10,
                            },
                            'hyperlinkDisplayType': 'LINKED',
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.fontSize,userEnteredFormat.hyperlinkDisplayType',
                }
            },
            {
                'repeatCell': {
                    'range': {
                        'sheetId': gamesettings_worksheet.id,
                        'startColumnIndex': df.get_column_index('Copy Paste'),
                        'startRowIndex': 1,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'fontSize': 9,
                            },
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.fontSize',
                }
            },
        ],
    )
    key = f'spreadsheets/PveRating.{prefix}_gamesettings.parquet'
    s3_upload_df(df, WRITE_DATA_BUCKET, key)

    payload = {
        'id': spreadsheet_id,
        'sheet_name': prefix + ' Gamesettings',
        'columns': [df.columns],
        'parquet_bucket': WRITE_DATA_BUCKET,
        'parquet_key': key,
        'batch_requests': batch_requests,
        'clear': not dev and random.randint(1, 10) == 1,
        'notes': {
            number_to_column_letter(df.get_column_index(a)): b
            for a, b in [
                (
                    'Merged Win Replays',
                    'Win replays with harder gamesettings',
                ),
                ('Merged Loss Replays', 'Loss replays with easier gamesettings'),
            ]
        },
    }
    del df

    invoke_lambda('Spreadsheet', payload)


def update_sheets(player_rating_df, prefix):
    spreadsheet_id = (
        '1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE'
        if dev
        else '18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY'
    )

    if dev:
        gc = gspread.service_account()
        pve_rating_spreadsheet = gc.open_by_key(spreadsheet_id)
    else:
        gc = gspread.service_account_from_dict(orjson.loads(get_secret()))
        pve_rating_spreadsheet = gc.open_by_key(spreadsheet_id)

    pve_rating_worksheet = get_or_create_worksheet(
        pve_rating_spreadsheet, prefix + ' PVE Rating'
    )

    percent_int_cols = [
        player_rating_df.columns.index(x)
        for x in [
            'Award Rate',
            'Difficulty Completion',
            'Difficulty Record',
            'Win Rate',
        ]
    ]

    weighted_award_rate_col_index = player_rating_df.columns.index(
        'Weighted Award Rate'
    )
    int_cols = [
        index for index, x in enumerate(player_rating_df.columns) if 'Rank' in x
    ] + [
        player_rating_df.columns.index(x)
        for x in ['Difficulty Losers Sum', '#Settings', '#Games']
    ]
    pve_rating_col_index = player_rating_df.columns.index('PVE Rating')

    batch_requests = [
        {
            'updateSpreadsheetProperties': {
                'properties': {
                    'title': f'{'[DEV] ' if dev else ''}PVE Rating, updated: {datetime.datetime.now(datetime.UTC):%Y-%m-%d %H:%M} UTC, by tetrisface',
                },
                'fields': 'title',
            }
        },
        {
            'repeatCell': {
                'range': {
                    'sheetId': pve_rating_worksheet.id,
                    'startColumnIndex': 0,
                    'endColumnIndex': 1,
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'LEFT',
                    }
                },
                'fields': 'userEnteredFormat.horizontalAlignment',
            }
        },
        *[
            {
                'repeatCell': {
                    'range': {
                        'sheetId': pve_rating_worksheet.id,
                        'startColumnIndex': col_index,
                        'endColumnIndex': col_index + 1,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'PERCENT',  # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#numberformattype
                                'pattern': '#%;#%;',  # https://developers.google.com/sheets/api/guides/formats#number_format_tokens
                            },
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat',  # NOSONAR
                }
            }
            for col_index in percent_int_cols
        ],
        *[
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startColumnIndex': col_index,
                        'endColumnIndex': col_index + 1,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '0.?;0.?;',  # https://developers.google.com/sheets/api/guides/formats#number_format_tokens
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat',
                }
            }
            for (sheet_id, col_index) in [
                (pve_rating_worksheet.id, weighted_award_rate_col_index),
                (pve_rating_worksheet.id, pve_rating_col_index),
            ]
            if col_index is not None
        ],
        *[
            {
                'repeatCell': {
                    'range': {
                        'sheetId': pve_rating_worksheet.id,
                        'startColumnIndex': col_index,
                        'endColumnIndex': col_index + 1,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '#',
                            },
                            'horizontalAlignment': 'RIGHT',
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat',
                }
            }
            for col_index in int_cols
        ],
        *[
            {
                'repeatCell': {
                    'range': {
                        'sheetId': pve_rating_worksheet.id,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'fontSize': 11,
                            },
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.fontSize',
                }
            },
            {
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': pve_rating_worksheet.id,
                        'dimension': 'COLUMNS',
                    },
                }
            },
            {
                'repeatCell': {
                    'range': {
                        'sheetId': pve_rating_worksheet.id,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'fontSize': 10,
                            },
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.fontSize',
                }
            },
        ],
    ]

    key = f'spreadsheets/PveRating.{prefix}_pve_rating.parquet'
    s3_upload_df(player_rating_df, WRITE_DATA_BUCKET, key)

    payload = {
        'id': spreadsheet_id,
        'sheet_name': prefix + ' PVE Rating',
        'columns': [player_rating_df.columns],
        'parquet_bucket': WRITE_DATA_BUCKET,
        'parquet_key': key,
        'batch_requests': batch_requests,
        'clear': True,
        'notes': {
            number_to_column_letter(player_rating_df.get_column_index(a)): b
            for a, b in [
                (
                    'Award Rate',
                    'Weight: 0\nEco and Damage award summed (1+1) for all games with more than 1 player divided by the count of those games',
                ),
                (
                    'Weighted Award Rate',
                    'Weight: 1\nSame as Award Rate but also multiplied by the number of teammates in each game',
                ),
                (
                    'Difficulty Record',
                    'Weight: ~0.075\nHighest difficulty won (winners/players)',
                ),
                (
                    'Difficulty Completion',
                    'Weight: ~0.075\nThe corresponding completion for the maximum value given by difficulty record * difficulty completion for each gamesetting. The completion is the amount of unique teammates in the gamesetting divided by a mapped value for each lobby size. Solo lobby win gives full completion. 16 player lobby wins requires 40 unique teammates. So 40% completion each 16 player win.',
                ),
                (
                    'Difficulty Losers Sum',
                    'Weight: 0.4\nSum of unique players that lost to gamesettings won by the player',
                ),
                ('#Settings', 'Weight: 0.01\nUnique settings'),
                ('#Games', 'Weight: 0.4\nCount of games from 0 to 20'),
                ('Win Rate', 'Weight: 0.005\nWins/Games'),
                (
                    'Difficulty Rank',
                    '(Difficulty Record * Difficulty Completion) ranked',
                ),
                ('Combined Rank', 'Sum of ranks multplied by their weights'),
                ('PVE Rating', 'Linear interpolation of Combined Rank'),
            ]
        },
    }
    invoke_lambda('Spreadsheet', payload)


if __name__ == '__main__':
    main({}, SimpleNamespace(function_name='PveRating'))
