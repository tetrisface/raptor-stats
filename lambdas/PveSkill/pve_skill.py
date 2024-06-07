import datetime
import re
import logging
import sys
import os
import json

import gspread
import numpy as np
import polars as pl
import polars.selectors as cs
from warnings import simplefilter

from Common.gamesettings import (
    gamesettings,
    lower_harder,
    higher_harder,
    possible_tweak_columns,
)
from Common.common import (
    get_df,
    get_secret,
    replay_details_file_name,
)
from Common.cast_frame import (
    add_computed_cols,
    cast_frame,
    reorder_tweaks,
)

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

simplefilter(action='ignore', category=pl.PolarsWarning)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()

gamesetting_equal_columns = set(possible_tweak_columns)
for gamesetting in gamesettings.values():
    gamesetting_equal_columns = gamesetting_equal_columns | set(gamesetting.keys())

gamesetting_equal_columns = (
    gamesetting_equal_columns - set(lower_harder) - set(higher_harder)
)

teammates_completion_fit_input, teammates_completion_fit_output = zip(
    *[
        (1, 1.15),
        (2, 4),
        (5, 12),
        (16, 25),
    ]
)
A = np.array([[x**2, x, 1] for x in teammates_completion_fit_input])
b = np.array(teammates_completion_fit_output)
teammates_coef_a, teammates_coef_b, teammates_coef_c = np.linalg.lstsq(
    A, b, rcond=None
)[0]


def main():
    _games = add_computed_cols(cast_frame(get_df(replay_details_file_name))).filter(
        ~pl.col('is_player_ai_mixed') & pl.col('has_player_handicap').eq(False)
    )

    process_games(_games.filter(pl.col('raptors').eq(True)), 'Raptors')
    process_games(_games.filter(pl.col('scavengers').eq(True)), 'Scavengers')


def process_games(games, prefix):
    if prefix == 'Raptors':
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
    ai_gamesetting_all_columns = sorted(
        ai_gamesetting_equal_columns
        | ai_gamesetting_lower_harder
        | ai_gamesetting_higher_harder
    )

    games = games.with_columns(
        pl.col('Map').struct.field('scriptName').alias('Map Name'),
        pl.lit([]).alias('Merged Win Replays'),
        winners=pl.col('winners').list.set_difference([ai_name]),
        players=pl.col('players').list.set_difference([ai_name]),
    ).with_columns(
        winners_extended=pl.col('winners'),
        players_extended=pl.col('players'),
    )

    logger.info(f'total replays {len(games)}')

    basic_player_aggregates = (
        games.unnest('damage_eco_award')
        .explode('players')
        .rename({'players': 'Player'})
        .group_by('Player')
        .agg(
            pl.col('Player').is_in('winners').mean().alias('Win Rate'),
            (
                pl.when(
                    pl.col('Player').eq(pl.col('damage_award'))
                    & pl.col('Player').is_in('winners')
                    & pl.col('players_extended').list.len().gt(1)
                )
                .then(1)
                .otherwise(0)
                + pl.when(
                    pl.col('Player').eq(pl.col('eco_award'))
                    & pl.col('Player').is_in('winners')
                    & pl.col('players_extended').list.len().gt(1)
                )
                .then(1)
                .otherwise(0)
            )
            .mean()
            .alias('Award Rate'),
            (
                (
                    (
                        pl.when(
                            pl.col('Player').eq(pl.col('damage_award'))
                            & pl.col('Player').is_in('winners')
                            & pl.col('players_extended').list.len().gt(1)
                        )
                        .then(1)
                        .otherwise(0)
                        + pl.when(
                            pl.col('Player').eq(pl.col('eco_award'))
                            & pl.col('Player').is_in('winners')
                            & pl.col('players_extended').list.len().gt(1)
                        )
                        .then(1)
                        .otherwise(0)
                    )
                    * (pl.col('players_extended').list.len() - 1)
                ).mean()
            ).alias('Weighted Award Rate'),
        )
    )

    # merge/coalesce players from harder into easier games
    for game in games.filter((pl.col(ai_win_column).eq(False))).iter_rows(named=True):
        # for game in games.filter(
        #     (pl.col('id').eq('a035616604a68c9d6815cbe3a2d25e40'))
        # ).iter_rows(named=True):
        if any(
            [
                v is None
                for k, v in game.items()
                if k
                in (
                    ai_gamesetting_equal_columns
                    | {'Map Name'}
                    | ai_gamesetting_lower_harder
                    | ai_gamesetting_higher_harder
                )
                - {'nuttyb_hp'}
            ]
        ):
            continue

        _ai_gamesetting_equal_columns = ai_gamesetting_equal_columns
        _ai_gamesetting_higher_harder = ai_gamesetting_higher_harder
        _ai_gamesetting_lower_harder = ai_gamesetting_lower_harder

        if game['evocom'] == 0:
            _ai_gamesetting_equal_columns = {
                x for x in ai_gamesetting_equal_columns if 'evocom' not in x
            }
            _ai_gamesetting_higher_harder = {
                x for x in ai_gamesetting_higher_harder if 'evocom' not in x
            }
            _ai_gamesetting_lower_harder = {
                x for x in ai_gamesetting_lower_harder if 'evocom' not in x
            }

        easier_games = games.filter(
            pl.col('id').ne(game['id']),
            *[
                # ((pl.col(x).is_null() & (game[x] is None)) | pl.col(x).eq(game[x]))
                pl.col(x).eq(game[x])
                for x in (_ai_gamesetting_equal_columns | {'Map Name'})
            ],
            *[pl.col(x).le(game[x]) for x in _ai_gamesetting_higher_harder],
            *[pl.col(x).ge(game[x]) for x in _ai_gamesetting_lower_harder],
        )

        if len(easier_games) == 0:
            # logger.info(f'no easier games for {game["id"]}')
            continue

        games = games.update(
            easier_games.with_columns(
                pl.col('Merged Win Replays')
                .list.concat(pl.lit([game['id']]))
                .alias('Merged Win Replays'),
                winners_extended=pl.col('winners').list.concat(pl.lit(game['winners'])),
                players_extended=pl.col('players').list.concat(pl.lit(game['players'])),
            ).select(
                'id',
                'winners_extended',
                'players_extended',
                'Merged Win Replays',
            ),
            on='id',
        )

    def teammates_weighted_success_rate(in_struct):
        logger.info(f'weighting {len(in_struct)} teammate success rates')
        winners = {
            x: {
                'success_rate': 1.0,
                'teammates_completion': 1.0,
                'teammates': set(),
                'wins': 0,
            }
            for x in set(
                [
                    winner
                    for gamesetting in in_struct
                    for winners in gamesetting['games_winners']
                    for winner in winners
                ]
            )
        }

        gamesetting_result_row_items = []
        for gamesetting in in_struct:
            gamesetting_result_row_item = winners.copy()
            success_rate_goal = gamesetting['Success Rate']
            for result_winner_name in gamesetting_result_row_item.keys():
                result_winner = gamesetting_result_row_item[result_winner_name]
                if result_winner['teammates_completion'] <= success_rate_goal:
                    continue
                for game_teammates in gamesetting['games_winners']:
                    game_teammates = set(game_teammates)
                    if result_winner_name not in game_teammates:
                        continue

                    new_teammates = (
                        game_teammates
                        - result_winner['teammates']
                        - {result_winner_name}
                    )
                    result_winner['teammates'] |= new_teammates
                    lobby_size = len(game_teammates)
                    n_new_teammates_and_me = len(new_teammates) + 1
                    teammates_completion_subtract = (1 - success_rate_goal) / (
                        (
                            teammates_coef_a * (lobby_size**2)
                            + teammates_coef_b * lobby_size
                            + teammates_coef_c
                        )
                        / n_new_teammates_and_me
                    )
                    result_winner['wins'] += 1
                    # logger.info(
                    #     f'{success_rate_goal:.2f}: {result_winner['success_rate']:.2f} -> {result_winner['success_rate'] - teammates_completion_subtract:.2f} new_teammates {n_new_teammates_and_me} lobby {lobby_size}'
                    # )
                    result_winner['success_rate'] -= teammates_completion_subtract
                    result_winner['teammates_completion'] = max(
                        success_rate_goal,
                        result_winner['success_rate']
                        - (
                            (1 - result_winner['success_rate'])
                            # / max(1, 2 / result_winner['wins'])
                            / 1
                        ),
                    )
                gamesetting_result_row_item[result_winner_name] = result_winner
            gamesetting_result_row_items.append(gamesetting_result_row_item)

        for gamesetting_result_row_item in gamesetting_result_row_items:
            for result_winner_name in gamesetting_result_row_item.keys():
                gamesetting_result_row_item[result_winner_name] = (
                    gamesetting_result_row_item[
                        result_winner_name
                    ]['teammates_completion']
                )
        return gamesetting_result_row_items

    group_by_columns = ['Map Name'] + list(ai_gamesetting_all_columns)
    group_df = (
        games.filter(pl.col('players_extended').len() > 0)
        .group_by(group_by_columns)
        .agg(
            (
                pl.col('winners_extended').flatten().drop_nulls().n_unique()
                / pl.col('players_extended').flatten().drop_nulls().n_unique()
            ).alias('Success Rate'),
            pl.col('winners_extended')
            .flatten()
            .drop_nulls()
            .unique()
            .sort()
            .alias('winners'),
            pl.col('winners_extended')
            .flatten()
            .drop_nulls()
            .n_unique()
            .alias('#Winners'),
            pl.col('players_extended')
            .flatten()
            .drop_nulls()
            .unique()
            .sort()
            .alias('Players'),
            pl.col('players_extended')
            .flatten()
            .drop_nulls()
            .n_unique()
            .alias('#Players'),
            pl.when(pl.col(ai_win_column).eq(False))
            .then(pl.col('id'))
            .drop_nulls()
            .unique()
            .alias('Win Replays'),
            pl.col('Merged Win Replays')
            .flatten()
            .drop_nulls()
            .unique()
            .alias('Merged Win Replays'),
            pl.when(pl.col(ai_win_column).ne(False))
            .then(pl.col('id'))
            .drop_nulls()
            .unique()
            .alias('Loss Replays'),
            pl.col('winners').flatten().drop_nulls().unique().alias('winners_flat'),
            pl.when(pl.col('winners').list.len() > 0)
            .then(pl.col('winners'))
            .drop_nulls()
            .alias('games_winners'),
            pl.col('winners')
            .flatten()
            .drop_nulls()
            .value_counts()
            .alias('winners_count'),
            pl.col('players')
            .flatten()
            .drop_nulls()
            .value_counts()
            .alias('players_count'),
        )
    )

    group_df = pl.concat(
        [
            group_df.filter((pl.col('Success Rate') == 0) & (pl.col('#Players') > 15))
            .sort('#Players', descending=True)
            .limit(15),
            group_df.filter(pl.col('Success Rate').is_between(0, 1, closed='none')),
            group_df.filter(pl.col('Success Rate') == 1)
            .sort('#Players', descending=True)
            .limit(10),
        ]
    ).with_columns(
        pl.struct(
            [
                pl.col('games_winners'),
                pl.col('Success Rate'),
            ]
        )
        .map_batches(lambda x: pl.Series(teammates_weighted_success_rate(x)))
        .alias('teammates_weighted_success_rate'),
    )

    logger.info('creating correlations')
    group_df = group_df.with_columns(
        group_df.select(group_by_columns)
        .select(cs.numeric())
        .transpose()
        .corr()
        .mean()
        .transpose()
        .to_series()
        .alias('Setting Correlations')
    )

    group_df = reorder_tweaks(group_df)

    logger.info('creating pastes')
    pastes = []
    for index, row in enumerate(group_df.iter_rows(named=True)):
        _str = '\n!preset coop\n' + (
            f'!map {row["Map Name"]}\n' if row['Map Name'] else ''
        )

        for key, value in row.items():
            if key not in ai_gamesetting_all_columns or value is None or value == '':
                continue

            value = re.sub('\\.0\\s*$', '', str(value))

            if ('multiplier_' in key and value == '1') or (
                'unit_restrictions_' in key and value == '0'
            ):
                continue

            if 'tweak' in key:
                _str += f'!bSet {key} {value}\n'
            else:
                _str += f'!{key} {re.sub("\\.0\\s*$", "", str(value))}\n'

        pastes.append(
            _str
            + f'$welcome-message Settings from http://docs.google.com/spreadsheets/d/{'1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE' if dev else '18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY'}#gid=0&range=I{index+2}\n'
        )

    logger.info('creating export df')
    group_export_df = (
        group_df.with_columns(
            pl.col('winners')
            .map_elements(
                lambda col: ', '.join(col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            )
            .alias('Winners'),
            pl.col('Players').map_elements(
                lambda col: ', '.join(col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Win Replays').map_elements(
                lambda col: ', '.join(col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Merged Win Replays').map_elements(
                lambda col: ', '.join(col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Loss Replays').map_elements(
                lambda col: ', '.join(col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.Series(pastes).alias('Copy Paste'),
        )
        .select(
            [
                'Success Rate',
                '#Winners',
                '#Players',
                'Winners',
                'Players',
                'Win Replays',
                'Merged Win Replays',
                'Loss Replays',
                'Copy Paste',
            ]
            + ['Map Name']
            + list(ai_gamesetting_all_columns)
        )
        .rename({'Map Name': 'Map'})
        .sort(by='Success Rate', descending=False)
    )

    logger.info('export group done')

    pve_rating_players = (
        group_df.explode('winners')
        .rename({'winners': 'Player'})
        .with_columns(
            pl.col('Player')
            .is_in(pl.col('winners_flat'))
            .alias('Setting Combinations'),
            pl.struct(
                pl.col('Player'),
                pl.col('Success Rate'),
                pl.col('teammates_weighted_success_rate'),
            )
            .map_elements(
                lambda x: x['teammates_weighted_success_rate'][x['Player']]
                if x['Player'] in x['teammates_weighted_success_rate']
                else 1.0
            )
            .alias('teammates_completion'),
        )
        .group_by('Player')
        .agg(
            pl.col('Success Rate').min().alias('Lowest Success Rate'),
            pl.col('teammates_completion').min().alias('S.R. Teammates Completion'),
            pl.col('Setting Combinations').sum(),
            pl.col('Setting Correlations').median(),
        )
        .join(basic_player_aggregates, on='Player', how='left', validate='1:1')
        .drop_nulls('Player')
        .with_columns(
            pl.col('Lowest Success Rate')
            .rank(descending=True)
            .alias('Lowest Success Rate Rank'),
            pl.col('S.R. Teammates Completion')
            .rank(descending=True)
            .alias('S.R. Teammates Completion Rank'),
            pl.col('Setting Combinations').rank().alias('Setting Combinations Rank'),
            pl.col('Setting Correlations')
            .rank(descending=True)
            .alias('Setting Correlations Rank'),
            pl.col('Win Rate').rank().alias('Win Rate Rank'),
            pl.col('Weighted Award Rate').rank().alias('Weighted Award Rate Rank'),
        )
        .with_columns(
            (
                pl.col('Lowest Success Rate Rank') * 0.5
                + pl.col('S.R. Teammates Completion Rank') * 1
                + pl.col('Setting Combinations Rank') * 0.2
                + pl.col('Setting Correlations Rank') * 0.0001
                + pl.col('Win Rate Rank') * 0.005
                + pl.col('Weighted Award Rate Rank') * 1
            ).alias('Combined Rank'),
        )
        .with_columns(
            (
                (
                    ((pl.col('Combined Rank')) - (pl.col('Combined Rank')).min())
                    / (
                        (pl.col('Combined Rank')).max()
                        - (pl.col('Combined Rank')).min()
                    )
                )
                * (30 - 0)
            ).alias('PVE Rating'),
        )
        .sort(by=['PVE Rating', 'Player'], descending=[True, False])
        .fill_nan('')
    )
    # pl.Config(tbl_rows=50, tbl_cols=111)

    logger.info('updating sheets')
    update_sheets(group_export_df, pve_rating_players, prefix=prefix)


def update_sheets(df, rating_number_df, prefix):
    if dev:
        gc = gspread.service_account()
        spreadsheet = gc.open_by_key('1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE')
    else:
        gc = gspread.service_account_from_dict(json.loads(get_secret()))
        spreadsheet = gc.open_by_key('18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY')

    worksheet_gamesettings = spreadsheet.worksheet(prefix + ' Gamesettings')
    # worksheet_gamesettings.clear()
    # worksheet_gamesettings.update(
    #     values=[['UPDATE IN PROGRESS']],
    #     value_input_option=gspread.utils.ValueInputOption.user_entered,
    # )
    logger.info(f'pushing {len(df)} {prefix} gamesettings')
    worksheet_gamesettings.update(
        values=[df.columns] + df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    del df
    worksheet_rating_number = spreadsheet.worksheet(prefix + ' PVE Rating')
    worksheet_rating_number.clear()
    logger.info(f'pushing {len(rating_number_df)} {prefix} pve player ratings')
    worksheet_rating_number.update(
        values=[rating_number_df.columns] + rating_number_df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    spreadsheet.batch_update(
        {
            'requests': [
                {
                    'updateSpreadsheetProperties': {
                        'properties': {
                            'title': f'{'[DEV] ' if dev else ''}PVE Rating, updated: {datetime.datetime.now(datetime.UTC):%Y-%m-%d %H:%M} UTC',
                        },
                        'fields': 'title',
                    }
                },
            ]
        }
    )


if __name__ == '__main__':
    main()
