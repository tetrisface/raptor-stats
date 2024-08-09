import io
import os
import re
from types import SimpleNamespace

import boto3
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
    invoke_lambda,
    READ_DATA_BUCKET,
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
from common.modoptions import modoptions

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

s()
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
                _games.filter(
                    'barbarian' & ~pl.col('raptors') & ~pl.col('scavengers')
                ).head(1000),
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
            pl.len().cast(pl.UInt16, strict=True).alias('n_games'),
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
        .with_columns(
            cs.matches(r'\sRate$').cast(pl.Float32, strict=True),
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
            pl.col('nuttyb_hp').is_null()
            if 'nuttyb_hp' in game and game['nuttyb_hp'] is None
            else True,
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
            )
            .cast(pl.Float32, strict=True)
            .alias('Difficulty'),
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
            .cast(pl.UInt16, strict=True)
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
            .cast(pl.UInt16, strict=True)
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
            pl.when(pl.col('winners_extended').list.len() > 0)
            .then(pl.col('winners_extended'))
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
        winners=pl.col('winners')
        .list.set_difference([prefix + 'AI'])
        .cast(pl.List(pl.UInt32), strict=True),
        players=pl.col('players')
        .list.set_difference([prefix + 'AI'])
        .cast(pl.List(pl.UInt32), strict=True),
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

    grouped_gamesettings_rating = (
        grouped_gamesettings_rating.rename({'Map Name': 'Map', 'winners': 'Winners'})
        .sort(by=['Difficulty', '#Players', 'Map'], descending=[True, True, False])
        .with_row_index()
        .cast({'index': pl.UInt16}, strict=True)
    )

    logger.info('Creating pastes')
    pastes = []
    for row in grouped_gamesettings_rating.iter_rows(named=True):
        row = {**non_unique_gamesetting_values, **row}
        _str = '!preset coop\n!draft_mode disabled\n!unit_market 1\n!teamsize 16\n' + (
            f'!map {row["Map"]}\n' if row['Map'] else ''
        )

        for key, value in row.items():
            value = str(round(value, 1) if '_spawntimemult' in key else value).strip()
            if (
                key in {'nuttyb_hp', 'Barbarian Handicap', 'Barbarian Per Player'}
                or key not in ai_gamesetting_all_columns
                or value is None
                or value == ''
                or modoptions.get(key, {}).get('def') == value
                or (
                    value in {'0', 0, '1', 1}
                    and bool(int(value)) == modoptions.get(key, {}).get('def')
                )
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

        nuttyb_link_str = (
            ' and https://docs.google.com/document/d/1ycQV-T__ilKeTKxbCyGjlTKw_6nmDSFdJo-kPmPrjIs'
            if 'nuttyb_hp' in row and row['nuttyb_hp'] is not None
            else ''
        )

        web_filter = 'regular'
        if row['Difficulty'] == 1:
            web_filter = 'unbeaten'
        elif row['Difficulty'] == 0:
            web_filter = 'easy'

        web_link = f'https://pverating.bar/?view=gamesettings&ai={prefix}&filter={web_filter}&row={row['index']}'
        pastes.append(
            _str
            + f'$welcome-message Settings from {web_link}{nuttyb_link_str}\n'
            + (
                f'$rename [Modded] {prefix}\n'
                if any(
                    v is not None and v != '' for k, v in row.items() if 'tweak' in k
                )
                else ''
            )
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
        'index',
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
    ]
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

    regular = grouped_gamesettings_export.filter(
        pl.col('Difficulty').is_between(difficulty_min, difficulty_max, closed='none')
    )
    unbeaten = grouped_gamesettings_export.filter(
        pl.col('Difficulty') == difficulty_max
    )

    regular_offset = -len(unbeaten)
    cheese_offset = -len(regular)

    s3_upload_df(
        regular,
        FILE_SERVE_BUCKET,
        prefix + '.regular.grouped_gamesettings.parquet',
    )
    s3_upload_df(
        unbeaten,
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
    del grouped_gamesettings_export

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

    del diff_tiered_export_limited, regular, unbeaten

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

    def player_gamesettings_diff_comp_agg(x):
        diff_completions = []
        diff_goals = []
        completions = []
        indices = []
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
            indices.append(
                player_gamesetting['index']
                + (regular_offset if difficulty_goal < 1 else 0)
                + (cheese_offset if difficulty_goal == 0 else 0)
            )

        # Get the indices of the top k values
        diff_completions = np.array(diff_completions)
        k = min(5, len(diff_completions))
        top_5_indices = np.argpartition(diff_completions, -k)[-k:]
        top_5_indices = top_5_indices[np.argsort(diff_completions[top_5_indices])][::-1]

        struct = {
            'diffs': [np.float32(diff_goals[i]) for i in top_5_indices],
            'completions': [np.float32(completions[i]) for i in top_5_indices],
            'diff_completions': [
                np.float32(diff_completions[i]) for i in top_5_indices
            ],
            'indices': [np.uint16(indices[i]) for i in top_5_indices],
        }
        return struct

    logger.info('Grouping by player and aggregating')
    grouped_gamesettings_rating = grouped_gamesettings_rating.group_by('Player').agg(
        pl.struct(
            pl.col('Player').alias('_player'),
            pl.col('games_winners_diff').explode(),
            pl.col('index'),
        )
        .map_elements(player_gamesettings_diff_comp_agg, return_dtype=pl.Struct)
        .alias('Top-5 Difficulties'),
        pl.when(pl.col('Player').is_in('winners_flat'))
        .then(pl.col('Players').list.set_difference(pl.col('winners_flat')))
        .otherwise(pl.lit([]))
        .flatten()
        .drop_nulls()
        .n_unique()
        .cast(pl.UInt16, strict=True)
        .alias('Difficulty Losers Sum'),
        pl.when(pl.col('Player').is_in('winners_flat'))
        .then(1)
        .otherwise(0)
        .sum()
        .cast(pl.UInt16, strict=True)
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
            (
                pl.col('Top-5 Difficulties').struct.field('diff_completions').list.sum()
                / 5
            )
            .cast(pl.Float32, strict=True)
            .alias('Difficulty Score'),
            pl.when(pl.col('n_games') > 20)
            .then(pl.lit('>20'))
            .otherwise(pl.col('n_games').clip(0, 20))
            .alias('#Games'),
            pl.col('Weighted Award Rate').rank().alias('Weighted Award Rate Rank'),
            (
                pl.col('Top-5 Difficulties').struct.field('diff_completions').list.sum()
                / 5
            )
            .rank()
            .alias('Difficulty Score Rank'),
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
                + pl.col('Difficulty Score Rank') * 0.25
                + pl.col('Setting Combinations Rank') * 0.01
                + pl.col('#Games Rank') * 0.5
                + pl.col('Win Rate Rank') * 0.005
            )
            .rank()
            .alias('Combined Rank'),
        )
        .with_columns(cs.matches(r' Rank$').cast(pl.UInt16, strict=True))
        .with_columns(
            (
                (
                    (pl.col('Combined Rank') - pl.col('Combined Rank').min())
                    / (pl.col('Combined Rank').max() - pl.col('Combined Rank').min())
                )
                * (30 - 0)
            )
            .cast(pl.Float32, strict=True)
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
        grouped_gamesettings_rating, 3, 'Difficulty Score'
    )
    grouped_gamesettings_rating = reorder_column(
        grouped_gamesettings_rating, 7, '#Games'
    )
    grouped_gamesettings_rating = reorder_column(
        grouped_gamesettings_rating, 8, 'Win Rate'
    )

    logger.info('Updating sheets grouped_gamesettings')

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


if __name__ == '__main__':
    main({}, SimpleNamespace(function_name='PveRating'))
