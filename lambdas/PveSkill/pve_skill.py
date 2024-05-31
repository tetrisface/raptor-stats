import re
import logging
import sys
import os
import json

import gspread
import polars as pl
from sklearn.preprocessing import MinMaxScaler
from warnings import simplefilter

from RaptorStats.gamesettings import (
    gamesettings,
    lower_harder,
    higher_harder,
    possible_tweak_columns,
)
from Common.common import get_df, get_secret, replay_details_file_name

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

simplefilter(action='ignore', category=pl.PolarsWarning)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main():
    raptors_games = get_df(replay_details_file_name).with_columns(
        pl.lit([]).alias('Merged Win Replays'),
        winners=pl.col('winners').list.set_difference(['RaptorsAI']),
        players=pl.col('players').list.set_difference(['RaptorsAI']),
        winners_extended=pl.col('winners').list.set_difference(['RaptorsAI']),
        players_extended=pl.col('players').list.set_difference(['RaptorsAI']),
    )
    logger.info(f'{len(raptors_games)} total replays')

    raptors_games = raptors_games.filter(
        pl.col('raptors').eq(True) & ~pl.col('is_player_ai_mixed')
    )
    # scavengers_games = df.filter(
    #     pl.col('scavengers').eq(True) & ~pl.col('is_player_ai_mixed')
    # )

    gamesetting_equal_columns = set(possible_tweak_columns)
    for gamesetting in gamesettings.values():
        gamesetting_equal_columns = gamesetting_equal_columns | set(gamesetting.keys())

    gamesetting_equal_columns = (
        gamesetting_equal_columns - set(lower_harder) - set(higher_harder)
    )

    gamesetting_all_columns = sorted(
        gamesetting_equal_columns | set(lower_harder) | set(higher_harder)
    )

    # coalesce players from harder into easier games
    for game in raptors_games.filter(
        pl.col('raptors_win').eq(False) & pl.col('draw').eq(False)
    ).iter_rows(named=True):
        easier_games = raptors_games.filter(
            [~pl.col('id').eq(game['id'])]
            + [pl.col(x).eq_missing(game[x]) for x in gamesetting_equal_columns]
            + [pl.col(x).le(game[x]) for x in higher_harder]
            + [pl.col(x).ge(game[x]) for x in lower_harder]
        )

        if len(easier_games) > 0:
            try:
                raptors_games = raptors_games.update(
                    easier_games.with_columns(
                        pl.col('Merged Win Replays')
                        .list.concat(pl.lit([game['id']]))
                        .alias('Merged Win Replays'),
                        winners_extended=pl.col('winners').list.concat(
                            pl.lit(game['winners'])
                        ),
                        players_extended=pl.col('players').list.concat(
                            pl.lit(game['players'])
                        ),
                    ).select(
                        'id',
                        'winners_extended',
                        'players_extended',
                        'Merged Win Replays',
                    ),
                    on='id',
                )

            except Exception as e:
                logger.exception(e)
                s()

    raptors_games = raptors_games.explode('players').rename({'players': 'player'})
    group_df = (
        raptors_games.filter(pl.col('players_extended').len() > 0)
        .group_by(list(gamesetting_all_columns))
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
            pl.when(pl.col('raptors_win').eq(False))
            .then(pl.col('id'))
            .drop_nulls()
            .unique()
            .alias('Win Replays'),
            pl.col('Merged Win Replays')
            .flatten()
            .drop_nulls()
            .unique()
            .alias('Merged Win Replays'),
            pl.when(pl.col('raptors_win').eq(True))
            .then(pl.col('id'))
            .drop_nulls()
            .unique()
            .alias('Loss Replays'),
        )
        .filter((pl.col('Success Rate') < 1) & (pl.col('Success Rate') > 0))
        .sort(by='Success Rate', descending=False)
    )

    pastes = []
    for row in group_df.iter_rows(named=True):
        pastes.append(
            '\n'
            + '\n'.join(
                [
                    f'!{key} {re.sub("\\.0\\s*$", "", str(value))}'
                    for key, value in row.items()
                    if key in gamesetting_all_columns
                ]
            )
            + '\n\n'
        )

    group_export_df = group_df.with_columns(
        pl.col('winners')
        .map_elements(lambda col: ', '.join(col.to_list()))
        .alias('Winners'),
        pl.col('Players').map_elements(lambda col: ', '.join(col.to_list())),
        pl.col('Win Replays').map_elements(lambda col: ', '.join(col.to_list())),
        pl.col('Merged Win Replays').map_elements(lambda col: ', '.join(col.to_list())),
        pl.col('Loss Replays').map_elements(lambda col: ', '.join(col.to_list())),
        pl.Series(pastes).alias('Copy Paste'),
    ).select(
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
        + list(gamesetting_all_columns)
    )

    pve_skill_players = (
        group_df.explode('winners')
        .rename({'winners': 'Player'})
        .group_by('Player')
        .agg(
            pl.col('Success Rate').min().alias('Lowest Success Rate'),
        )
    )

    min_max_scaler = MinMaxScaler(feature_range=(0, 99))
    min_max_scaler.set_output(transform='polars')

    pve_skill_players = pve_skill_players.with_columns(
        min_max_scaler.fit_transform(
            (1 - pve_skill_players['Lowest Success Rate']).to_numpy().reshape(-1, 1)
        )
        .to_series()
        .round(0)
        .cast(pl.UInt8)
        .alias('PVE Skill'),
    ).sort(by=['PVE Skill', 'Player'], descending=[True, False])

    update_sheets(group_export_df, pve_skill_players)


def update_sheets(df, skill_number_df):
    if len(df) == 0:
        return

    data_values = df.rows()
    values = [df.columns] + data_values

    if dev:
        gc = gspread.service_account()
    else:
        gc = gspread.service_account_from_dict(json.loads(get_secret()))

    spreadsheet = (
        gc.open_by_key('1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE')
        if dev
        else gc.open_by_key('18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY')
    )
    worksheet_gamesettings = spreadsheet.worksheet('gamesettings grouped')
    worksheet_gamesettings.clear()
    worksheet_gamesettings.update(
        values=values,
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    del values, data_values, df

    if len(skill_number_df) == 0:
        return

    data_values = skill_number_df.rows()
    values = [skill_number_df.columns] + data_values

    worksheet_skill_number = spreadsheet.worksheet('skill number')
    worksheet_skill_number.clear()
    worksheet_skill_number.update(
        values=values,
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )


if __name__ == '__main__':
    main()
