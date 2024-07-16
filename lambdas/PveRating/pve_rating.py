import datetime
import io
import os
import random
import re
import warnings

import boto3
import gspread
import numpy as np
import orjson
import polars as pl
import polars.selectors as cs
import s3fs
from Common.cast_frame import (
    add_computed_cols,
    cast_frame,
    reorder_column,
    reorder_tweaks,
)
from Common.common import (
    WRITE_DATA_BUCKET,
    get_df,
    get_secret,
    replay_details_file_name,
)
from Common.gamesettings import (
    gamesetting_equal_columns,
    higher_harder,
    lower_harder,
    possible_tweak_columns,
)
from Common.logger import get_logger
from Spreadsheet.spreadsheet import get_or_create_worksheet

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

# Interpolation with quadratic fit
lobby_size_teammates_completion_fit_input = [1, 2, 5, 16]
lobby_size_teammates_completion_fit_output = [1, 4, 14, 40]
A = np.array([[x**2, x, 1] for x in lobby_size_teammates_completion_fit_input])
b = np.array(lobby_size_teammates_completion_fit_output)
(
    lobby_size_teammates_coef_a,
    lobby_size_teammates_coef_b,
    lobby_size_teammates_coef_c,
) = np.linalg.lstsq(A, b, rcond=None)[0]


def main():
    _games = (
        add_computed_cols(cast_frame(get_df(replay_details_file_name)))
        .filter(
            ~pl.col('is_player_ai_mixed')
            & pl.col('has_player_handicap').eq(False)
            & pl.col('draw').eq(False)
        )
        .drop('is_player_ai_mixed', 'has_player_handicap', 'draw')
    )

    json_data = {
        'pve_ratings': {
            'barbarian': process_games(
                _games.filter('barbarian' & ~pl.col('raptors') & ~pl.col('scavengers')),
                'Barbarian',
            ),
            'raptors': process_games(
                _games.filter('raptors' & ~pl.col('scavengers') & ~pl.col('barbarian')),
                'Raptors',
            ),
            'scavengers': process_games(
                _games.filter('scavengers' & ~pl.col('raptors') & ~pl.col('barbarian')),
                'Scavengers',
            ),
        }
    }

    if not dev:
        s3 = boto3.client('s3')

        with io.BytesIO() as buffer:
            buffer.write(orjson.dumps(json_data))
            buffer.seek(0)
            s3.put_object(Bucket='pve-rating-web', Key='pve_ratings.json', Body=buffer)


def process_games(games, prefix):  # NOSONAR
    if prefix == 'Barbarian':
        ai_name = 'BarbarianAI'
        ai_win_column = 'barbarian_win'
        ai_gamesetting_equal_columns = {
            'comrespawn',
            'evocom',
            'evocomlevelcap',
            'evocomlevelupmethod',
            'evocomleveluprate',
            'evocomxpmultiplier',
            'experimentalextraunits',
            'experimentallegionfaction',
            'forceallunits',
            'lootboxes_density',
            'lootboxes',
            'multiplier_builddistance',
            'multiplier_buildpower',
            'multiplier_buildtimecost',
            'multiplier_energyconversion',
            'multiplier_energycost',
            'multiplier_energyproduction',
            'multiplier_losrange',
            'multiplier_maxdamage',
            'multiplier_maxvelocity',
            'multiplier_metalcost',
            'multiplier_metalextraction',
            'multiplier_radarrange',
            'multiplier_resourceincome',
            'multiplier_shieldpower',
            'multiplier_turnrate',
            'multiplier_weapondamage',
            'multiplier_weaponrange',
            'ruins_civilian_disable',
            'ruins_density',
            'ruins_only_t1',
            'ruins',
            'startenergy',
            'startenergystorage',
            'startmetal',
            'startmetalstorage',
            'unit_restrictions_noair',
            'unit_restrictions_noconverters',
            'unit_restrictions_noendgamelrpc',
            'unit_restrictions_noextractors',
            'unit_restrictions_nolrpc',
            'unit_restrictions_nonukes',
            'unit_restrictions_notacnukes',
            'unit_restrictions_notech2',
            'unit_restrictions_notech3',
        } | set(possible_tweak_columns)
        ai_gamesetting_lower_harder = {
            'disable_fogofwar',
        }
        ai_gamesetting_higher_harder = {'Barbarian Per Player', 'Barbarian Handicap'}
        games = games.filter(
            ~pl.col('Barbarian Per Player').is_infinite(),
            *[pl.col(col).eq(1) for col in games.columns if 'multiplier_' in col],
        )
    elif prefix == 'Raptors':
        ai_name = 'RaptorsAI'
        ai_win_column = 'raptors_win'
        ai_gamesetting_equal_columns = {
            x for x in gamesetting_equal_columns if 'scav_' not in x
        }
        ai_gamesetting_lower_harder = {x for x in lower_harder if 'scav_' not in x}
        ai_gamesetting_higher_harder = {x for x in higher_harder if 'scav_' not in x}
    elif prefix == 'Scavengers':
        ai_name = 'ScavengersAI'
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

    logger.info('Making user id->player name mapping')
    cols = (
        games.sort('startTime', descending=False)
        .select(
            pl.col('AllyTeams')
            .list.eval(
                pl.element()
                .struct['Players']
                .list.eval(
                    pl.struct(
                        pl.element().struct['userId'].cast(pl.UInt32),
                        pl.element().struct['name'],
                    )
                )
                .flatten()
                .drop_nulls()
            )
            .explode()
        )
        .unnest('AllyTeams')
        .to_dict()
    )
    user_ids_names = {k: v for (k, v) in zip(cols['userId'], cols['name'])}
    games = games.drop('AllyTeams', 'AllyTeamsList')

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

    games = games.with_columns(
        pl.lit([]).alias('Merged Win Replays'),
        pl.lit([]).alias('Merged Loss Replays'),
        winners=pl.col('winners').list.set_difference([ai_name]),
        players=pl.col('players').list.set_difference([ai_name]),
    ).with_columns(
        winners_extended=pl.col('winners'),
        players_extended=pl.col('players'),
    )

    award_sum_expression = pl.when(pl.col('players_extended').list.len().gt(1)).then(
        pl.when(pl.col('Player').eq(pl.col('damage_award'))).then(1).otherwise(0)
        + pl.when(pl.col('Player').eq(pl.col('eco_award'))).then(1).otherwise(0)
    )

    logger.info('Basic player aggregates')
    basic_player_aggregates = (
        games.unnest('damage_eco_award')
        .explode('players')
        .rename({'players': 'Player'})
        .group_by('Player')
        .agg(
            pl.len().alias('n_games'),
            pl.col('Player').is_in('winners').mean().alias('Win Rate'),
            pl.when(pl.col('Player').is_in('winners'))
            .then(award_sum_expression)
            .otherwise(None)
            .drop_nulls()
            .mean()
            .alias('Award Rate'),
            pl.when(pl.col('Player').is_in('winners'))
            .then(award_sum_expression * (pl.col('players_extended').list.len() - 1))
            .otherwise(None)
            .drop_nulls()
            .mean()
            .alias('Weighted Award Rate'),
        )
    )

    # merge/coalesce players from harder into easier games
    logger.info('Merg/coalesce players from harder games into easier games')
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

        if game['evocom'] == 0:
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
        if game['commanderbuildersenabled'] == 'disabled':
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
        if game['assistdronesenabled'] == 'disabled':
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

        if game['nuttyb_hp'] is None:
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
        games.group_by(group_by_columns)
        .agg(
            (
                1
                - (
                    pl.col('winners_extended').flatten().drop_nulls().n_unique()
                    / pl.col('players_extended').flatten().drop_nulls().n_unique()
                )
            ).alias('Difficulty'),
            pl.col('winners_extended')
            .sort_by('startTime', descending=False)
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
            .sort_by('startTime', descending=False)
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
            .then(pl.col('id').sort_by('startTime', descending=False))
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Win Replays'),
            pl.col('Merged Win Replays')
            .sort_by('startTime', descending=False)
            .flatten()
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Merged Win Replays'),
            pl.col('Merged Loss Replays')
            .sort_by('startTime', descending=False)
            .flatten()
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Merged Loss Replays'),
            pl.when(pl.col(ai_win_column).ne(False))
            .then(pl.col('id').sort_by('startTime', descending=False))
            .drop_nulls()
            .unique(maintain_order=True)
            .alias('Loss Replays'),
            pl.col('winners').flatten().drop_nulls().unique().alias('winners_flat'),
            pl.when(pl.col('winners').list.len() > 0)
            .then(pl.col('winners'))
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
    del games

    grouped_gamesettings = reorder_tweaks(grouped_gamesettings)

    logger.info('Creating export df')
    group_df_sample = pl.concat(
        [
            grouped_gamesettings.filter(
                (pl.col('Difficulty') == 1) & (pl.col('#Players') > 15)
            )
            .sort('#Players', descending=True)
            .limit(15),
            grouped_gamesettings.filter(
                pl.col('Difficulty').is_between(0, 1, closed='none')
            ),
            grouped_gamesettings.filter(pl.col('Difficulty') == 0)
            .sort('#Players', descending=True)
            .limit(10),
        ]
    )

    logger.info('Creating pastes')
    pastes = []
    for index, row in enumerate(group_df_sample.iter_rows(named=True)):
        _str = '\n!preset coop\n!draft_mode disabled\n!unit_market 1\n' + (
            f'!map {row["Map Name"]}\n' if row['Map Name'] else ''
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

    group_export_df = (
        group_df_sample.with_columns(
            pl.col('winners')
            .map_elements(
                lambda _col: ', '.join(
                    _col.replace_strict(
                        user_ids_names, return_dtype=pl.String
                    ).to_list()
                ),
                skip_nulls=True,
                return_dtype=pl.String,
            )
            .alias('Winners'),
            pl.col('Players').map_elements(
                lambda _col: ', '.join(
                    _col.replace_strict(
                        user_ids_names, return_dtype=pl.String
                    ).to_list()
                ),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Win Replays').map_elements(
                lambda _col: ', '.join(_col.cast(str).to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Merged Win Replays').map_elements(
                lambda _col: ', '.join(_col.cast(str).to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Loss Replays').map_elements(
                lambda _col: ', '.join(_col.cast(str).to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Merged Loss Replays').map_elements(
                lambda _col: ', '.join(_col.cast(str).to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.Series(pastes).alias('Copy Paste'),
        )
        .select(
            [
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
            ]
            + ['Map Name']
            + list(ai_gamesetting_all_columns)
        )
        .fill_null(' ')
        .rename({'Map Name': 'Map'})
        .sort(by=['Difficulty', '#Players', 'Map'], descending=[True, True, False])
    )
    push_gamesettings_export_df(group_export_df, prefix)
    del group_df_sample, pastes, group_export_df

    logger.info('Creating pve ratings')

    grouped_gamesettings = grouped_gamesettings.filter(
        pl.col('Difficulty').is_between(0, 1, closed='none')
    )

    grouped_gamesettings = grouped_gamesettings.sort(by='#Players', descending=True)
    logger.info(
        f'gamesettings team completions iteration gs {len(grouped_gamesettings)} min players {grouped_gamesettings["#Players"][-1]}'
    )
    logger.info('Adding teammates weighted success rate + explode winners')

    grouped_gamesettings = (
        grouped_gamesettings.with_columns(
            pl.struct('games_winners', 'Difficulty').alias('games_winners_diff'),
        )
        .explode('winners')
        .rename({'winners': 'Player'})
    )

    logger.info('Adding teammates completion float')

    def diff_comp(x):
        diff_completions = []
        for player_gamesetting in x:
            winner_name = player_gamesetting['_player']
            gamesetting = player_gamesetting['']
            difficulty_goal = gamesetting['Difficulty']

            completion = 0.0
            total_teammates = set()
            for game_teammates in gamesetting['games_winners']:
                if completion >= difficulty_goal:
                    break
                game_teammates = set(game_teammates)
                if winner_name not in game_teammates:
                    continue

                new_teammates = game_teammates - total_teammates - {winner_name}
                total_teammates |= new_teammates
                lobby_size = len(game_teammates)
                n_new_teammates_and_me = len(new_teammates) + 1
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
                    difficulty_goal,
                    completion + teammates_completion_addition,
                )
            diff_completions.append(difficulty_goal * completion)

        return max(diff_completions) if len(diff_completions) > 0 else None

    logger.info('Grouping by player and aggregating')
    grouped_gamesettings = grouped_gamesettings.group_by('Player').agg(
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

    logger.info('Continuing')
    grouped_gamesettings = (
        grouped_gamesettings.join(
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
            pl.col('Difficulty Completion').rank().alias('Difficulty Completion Rank'),
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
                + pl.col('Difficulty Completion Rank') * 0.15
                + pl.col('Setting Combinations Rank') * 0.01
                + pl.col('#Games Rank') * 0.4
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

    grouped_gamesettings = reorder_column(grouped_gamesettings, 1, 'Award Rate')
    grouped_gamesettings = reorder_column(
        grouped_gamesettings, 2, 'Weighted Award Rate'
    )
    grouped_gamesettings = reorder_column(grouped_gamesettings, 7, '#Games')
    grouped_gamesettings = reorder_column(grouped_gamesettings, 8, 'Win Rate')

    logger.info('Updating sheets')
    update_sheets(grouped_gamesettings, prefix=prefix)
    return {
        player: rating
        for player, rating in zip(
            *grouped_gamesettings.select('Player', 'PVE Rating')
            .to_dict(as_series=False)
            .values()
        )
    }


def push_gamesettings_export_df(df, prefix):
    path = os.path.join(
        WRITE_DATA_BUCKET, f'spreadsheets/PveRating.{prefix}_gamesettings.parquet'
    )
    logger.info(f'Writing {len(df)} rows to {path}')
    fs = s3fs.S3FileSystem()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=UserWarning)
        with fs.open(path, 'wb') as f:
            df.write_parquet(f)

    if dev:
        spreadsheet_id = '1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE'
        gc = gspread.service_account()
        pve_rating_spreadsheet = gc.open_by_key(spreadsheet_id)
    else:
        spreadsheet_id = '18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY'
        gc = gspread.service_account_from_dict(orjson.loads(get_secret()))
        pve_rating_spreadsheet = gc.open_by_key(spreadsheet_id)
    gamesettings_worksheet = get_or_create_worksheet(
        pve_rating_spreadsheet, prefix + ' Gamesettings'
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
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.fontSize',
                }
            },
        ],
    )

    payload = {
        'id': spreadsheet_id,
        'sheet_name': prefix + ' Gamesettings',
        'columns': [df.columns],
        'parquet_path': path,
        'batch_requests': batch_requests,
        'clear': not dev and random.randint(1, 10) % 10 == 0,
    }
    del df

    if dev:
        import Spreadsheet.spreadsheet as _spreadsheet

        logger.info(f'Invoking local Spreadsheet {prefix} Gamesettings')
        _spreadsheet.main(payload)
    else:
        lambda_client = boto3.client('lambda')

        logger.info(f'Invoking Spreadsheet {prefix} Gamesettings')
        lambda_client.invoke(
            FunctionName='Spreadsheet',
            InvocationType='Event',
            Payload=orjson.dumps(payload),
        )


def update_sheets(rating_number_df, prefix):
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
        rating_number_df.columns.index(x)
        for x in [
            'Award Rate',
            'Difficulty Completion',
            'Difficulty Record',
            'Win Rate',
        ]
    ]

    weighted_award_rate_col_index = rating_number_df.columns.index(
        'Weighted Award Rate'
    )
    int_cols = [
        index for index, x in enumerate(rating_number_df.columns) if 'Rank' in x
    ] + [
        rating_number_df.columns.index(x)
        for x in ['Difficulty Losers Sum', '#Settings', '#Games']
    ]
    pve_rating_col_index = rating_number_df.columns.index('PVE Rating')

    batch_requests = [
        {
            'updateSpreadsheetProperties': {
                'properties': {
                    'title': f'{'[DEV] ' if dev else ''}PVE Rating, updated: {datetime.datetime.now(datetime.UTC):%Y-%m-%d %H:%M} UTC',
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

    path = os.path.join(
        WRITE_DATA_BUCKET, f'spreadsheets/PveRating.{prefix}_pve_rating.parquet'
    )
    fs = s3fs.S3FileSystem()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=UserWarning)
        with fs.open(
            path,
            'wb',
        ) as f:
            rating_number_df.write_parquet(f)

    payload = {
        'id': spreadsheet_id,
        'sheet_name': prefix + ' PVE Rating',
        'columns': [rating_number_df.columns],
        'parquet_path': path,
        'batch_requests': batch_requests,
        'clear': True,
    }
    if dev:
        import Spreadsheet.spreadsheet as _spreadsheet

        logger.info(f'Invoking local Spreadsheet {prefix} PVE Rating')
        _spreadsheet.main(payload)
    else:
        lambda_client = boto3.client('lambda')
        logger.info(f'Invoking Spreadsheet {prefix} PVE Rating')
        lambda_client.invoke(
            FunctionName='Spreadsheet',
            InvocationType='Event',
            Payload=orjson.dumps(payload),
        )


if __name__ == '__main__':
    main()
