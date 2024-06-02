import re
import logging
import sys
import os
import json

import gspread
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
from Common.cast_frame import cast_frame

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

simplefilter(action='ignore', category=pl.PolarsWarning)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main():
    _games = cast_frame(get_df(replay_details_file_name)).filter(
        ~pl.col('is_player_ai_mixed') & pl.col('has_player_handicap').eq(False)
    )

    process_games(_games.filter(pl.col('raptors').eq(True)), 'Raptors')
    process_games(_games.filter(pl.col('scavengers').eq(True)), 'Scavengers')


def process_games(games, prefix):
    games = games.with_columns(
        pl.col('Map').struct.field('scriptName').alias('Map Name'),
        pl.lit([]).alias('Merged Win Replays'),
        winners=pl.col('winners').list.set_difference(['RaptorsAI'])
        if prefix == 'Raptors'
        else pl.col('winners').list.set_difference(['ScavengersAI']),
        players=pl.col('players').list.set_difference(['RaptorsAI'])
        if prefix == 'Raptors'
        else pl.col('players').list.set_difference(['ScavengersAI']),
    ).with_columns(
        winners_extended=pl.col('winners'),
        players_extended=pl.col('players'),
    )

    logger.info(f'total replays {len(games)}')

    gamesetting_equal_columns = set(possible_tweak_columns)
    for gamesetting in gamesettings.values():
        gamesetting_equal_columns = gamesetting_equal_columns | set(gamesetting.keys())

    gamesetting_equal_columns = (
        gamesetting_equal_columns - set(lower_harder) - set(higher_harder)
    )

    gamesetting_all_columns = sorted(
        (gamesetting_equal_columns) | set(lower_harder) | set(higher_harder)
    )

    # merge/coalesce players from harder into easier games
    for game in games.filter(
        pl.col('raptors_win').eq(False) & pl.col('scavengers_win').eq(False)
    ).iter_rows(named=True):
        easier_games = games.filter(
            [~pl.col('id').eq(game['id'])]
            + [
                pl.col(x).eq_missing(game[x])
                for x in gamesetting_equal_columns | {'Map Name'}
            ]
            + [pl.col(x).le(game[x]) for x in higher_harder]
            + [pl.col(x).ge(game[x]) for x in lower_harder]
        )

        if len(easier_games) == 0:
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
            x: {'teammates': set(), 'success_rate': 1.0}
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
            success_rate = gamesetting['Success Rate']
            for result_winner in gamesetting_result_row_item.keys():
                if (
                    gamesetting_result_row_item[result_winner]['success_rate']
                    <= success_rate
                ):
                    continue
                for teammates in gamesetting['games_winners']:
                    teammates = set(teammates)
                    if result_winner not in teammates:
                        continue

                    new_teammates = (
                        teammates
                        - gamesetting_result_row_item[result_winner]['teammates']
                        - {result_winner}
                    )
                    if len(new_teammates) == 0 and len(teammates) != 1:
                        continue
                    x = len(new_teammates) + 1
                    gamesetting_result_row_item[result_winner]['success_rate'] = max(
                        success_rate,
                        gamesetting_result_row_item[result_winner]['success_rate']
                        - success_rate / min(1, -0.0603 * (x**2) + 2.6371 * x - 1.7648),
                    )
                    gamesetting_result_row_item[result_winner]['teammates'] |= (
                        new_teammates
                    )
            gamesetting_result_row_items.append(gamesetting_result_row_item)

        for gamesetting_result_row_item in gamesetting_result_row_items:
            for result_winner in gamesetting_result_row_item.keys():
                if 'teammates' in gamesetting_result_row_item[result_winner]:
                    del gamesetting_result_row_item[result_winner]['teammates']

        logger.info(
            f'weighting {len(gamesetting_result_row_items)} teammate success rates done'
        )
        return gamesetting_result_row_items

    group_by_columns = ['Map Name'] + list(gamesetting_all_columns)
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
            pl.when(
                pl.col('raptors_win').eq(False) | pl.col('scavengers_win').eq(False)
            )
            .then(pl.col('id'))
            .drop_nulls()
            .unique()
            .alias('Win Replays'),
            pl.col('Merged Win Replays')
            .flatten()
            .drop_nulls()
            .unique()
            .alias('Merged Win Replays'),
            pl.when(pl.col('raptors_win').eq(True) | pl.col('scavengers_win').eq(True))
            .then(pl.col('id'))
            .drop_nulls()
            .unique()
            .alias('Loss Replays'),
            pl.when(pl.col('winners').list.len() > 0)
            .then(pl.col('winners'))
            .drop_nulls()
            .alias('games_winners'),
            pl.col('winners').flatten().drop_nulls().unique().alias('winners_flat'),
        )
        .filter((pl.col('Success Rate') < 1) & (pl.col('Success Rate') > 0))
        .with_columns(
            teammates_weighted_success_rate=pl.struct(
                [pl.col('games_winners'), pl.col('Success Rate')]
            ).map_batches(lambda x: pl.Series(teammates_weighted_success_rate(x)))
        )
    )

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

    pastes = []
    for row in group_df.iter_rows(named=True):
        pastes.append(
            '\n!preset coop\n'
            + (f'!map {row["Map Name"]}\n' if row['Map Name'] else '')
            + '\n'.join(
                [
                    f'!{key} {re.sub("\\.0\\s*$", "", str(value))}'
                    for key, value in row.items()
                    if key in gamesetting_all_columns
                ]
            )
            + '\n\n'
        )

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
            + list(gamesetting_all_columns)
        )
        .rename({'Map Name': 'Map'})
        .sort(by='Success Rate', descending=False)
    )

    logger.info('export group done')

    pve_skill_players = (
        group_df.explode('winners')
        .rename({'winners': 'Player'})
        .with_columns(
            pl.col('Player')
            .is_in(pl.col('winners_flat'))
            .alias('Setting Combinations'),
            pl.struct(
                [
                    pl.col('Player'),
                    pl.col('Success Rate'),
                    pl.col('teammates_weighted_success_rate'),
                ]
            )
            .map_elements(
                lambda x: x['teammates_weighted_success_rate'][x['Player']]
                if x['Player'] in x['teammates_weighted_success_rate']
                else {'success_rate': x['Success Rate']}
            )
            .alias('unnest_struct'),
        )
        .unnest('unnest_struct')
        .group_by('Player')
        .agg(
            pl.col('Success Rate').min().alias('Lowest Success Rate'),
            pl.col('success_rate').min().alias('Teammates Completion'),
            pl.col('Setting Combinations').sum(),
            pl.col('Setting Correlations').median(),
        )
        .with_columns(
            pl.col('Lowest Success Rate')
            .rank(descending=True)
            .alias('Lowest Success Rate Rank'),
            pl.col('Teammates Completion')
            .rank(descending=True)
            .alias('Teammates Completion Rank'),
            pl.col('Setting Combinations').rank().alias('Setting Combinations Rank'),
            pl.col('Setting Correlations')
            .rank(descending=True)
            .alias('Setting Correlations Rank'),
        )
        .with_columns(
            (
                pl.col('Lowest Success Rate Rank')
                + pl.col('Teammates Completion Rank') * 0.2
                + pl.col('Setting Combinations Rank') * 0.2
                + pl.col('Setting Correlations Rank') * 0.01
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
                * (50 - 0)
            ).alias('PVE Skill'),
        )
        .sort(by=['PVE Skill', 'Player'], descending=[True, False])
    )

    logger.info('updating sheets')

    update_sheets(group_export_df, pve_skill_players, prefix=prefix)


def update_sheets(df, skill_number_df, prefix):
    if dev:
        gc = gspread.service_account()
        spreadsheet = gc.open_by_key('1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE')
    else:
        gc = gspread.service_account_from_dict(json.loads(get_secret()))
        spreadsheet = gc.open_by_key('18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY')

    worksheet_gamesettings = spreadsheet.worksheet(prefix + ' Gamesettings')
    worksheet_gamesettings.clear()
    worksheet_gamesettings.update(
        values=[df.columns] + df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    del df

    worksheet_skill_number = spreadsheet.worksheet(prefix + ' PVE Skill')
    worksheet_skill_number.clear()
    worksheet_skill_number.update(
        values=[skill_number_df.columns] + skill_number_df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )


if __name__ == '__main__':
    main()
