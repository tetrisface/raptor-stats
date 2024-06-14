from dataclasses import dataclass
import datetime
import io
import boto3
import random
import re
import logging
import sys
import os

import gspread
import numpy as np
import orjson
import polars as pl
import polars.selectors as cs

from Common.gamesettings import (
    gamesetting_equal_columns,
    lower_harder,
    higher_harder,
)
from Common.common import (
    get_df,
    get_secret,
    replay_details_file_name,
)
from Common.cast_frame import (
    add_computed_cols,
    cast_frame,
    reorder_column,
    reorder_tweaks,
)

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

teammates_completion_fit_input, teammates_completion_fit_output = zip(
    *[
        (1, 1),
        (2, 4),
        (5, 14),
        (16, 40),
    ]
)
A = np.array([[x**2, x, 1] for x in teammates_completion_fit_input])
b = np.array(teammates_completion_fit_output)
teammates_coef_a, teammates_coef_b, teammates_coef_c = np.linalg.lstsq(
    A, b, rcond=None
)[0]


@dataclass
class PveRatingColumnNames:
    AwardRate: str = 'Award Rate'  # NOSONAR
    CombinedRank: str = 'Combined Rank'  # NOSONAR
    CopyPaste: str = 'Copy Paste'  # NOSONAR
    DifficultyCompletion: str = 'Difficulty Completion'  # NOSONAR
    DifficultyCompletionRank: str = 'Difficulty Completion Rank'  # NOSONAR
    DifficultyRecord: str = 'Difficulty Record'  # NOSONAR
    games_winners: str = 'games_winners'  # NOSONAR
    LossReplays: str = 'Loss Replays'  # NOSONAR
    MapName: str = 'Map Name'  # NOSONAR
    MergedLossReplays: str = 'Merged Loss Replays'  # NOSONAR
    MergedWinReplays: str = 'Merged Win Replays'  # NOSONAR
    n_games: str = 'n_games'  # NOSONAR
    nGames: str = '#Games'  # NOSONAR
    nGamesRank: str = '#Games Rank'  # NOSONAR
    nPlayers: str = '#Players'  # NOSONAR
    nWinners: str = '#Winners'  # NOSONAR
    players_count: str = 'players_count'  # NOSONAR
    Players: str = 'Players'  # NOSONAR
    PVERating: str = 'PVE Rating'  # NOSONAR
    SettingCombinations: str = 'Setting Combinations'  # NOSONAR
    SettingCombinationsRank: str = 'Setting Combinations Rank'  # NOSONAR
    SettingCorr: str = 'Setting Corr.'  # NOSONAR
    SettingCorrRank: str = 'Setting Corr. Rank'  # NOSONAR
    SuccessRate: str = 'Success Rate'  # NOSONAR
    teammates_completion: str = 'teammates_completion'  # NOSONAR
    teammates_weighted_success_rate: str = 'teammates_weighted_success_rate'  # NOSONAR
    WeightedAwardRate: str = 'Weighted Award Rate'  # NOSONAR
    WeightedAwardRateRank: str = 'Weighted Award Rate Rank'  # NOSONAR
    winners_count: str = 'winners_count'  # NOSONAR
    winners_flat: str = 'winners_flat'  # NOSONAR
    winners: str = 'winners'  # NOSONAR
    Winners: str = 'Winners'  # NOSONAR
    WinRate: str = 'Win Rate'  # NOSONAR
    WinRateRank: str = 'Win Rate Rank'  # NOSONAR
    WinReplays: str = 'Win Replays'  # NOSONAR


col = PveRatingColumnNames()


def main():
    _games = add_computed_cols(cast_frame(get_df(replay_details_file_name))).filter(
        ~pl.col('is_player_ai_mixed')
        & pl.col('has_player_handicap').eq(False)
        & pl.col('startTime')
        .dt.datetime()
        .gt(datetime.datetime.now() - datetime.timedelta(days=120))
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

    null_columns_df = (
        games[
            list(
                set(ai_gamesetting_all_columns + ['Map'])
                - {'nuttyb_hp', 'multiplier_maxdamage'}
            )
        ]
        .null_count()
        .transpose(include_header=True, header_name='setting', column_names=['value'])
        .filter(pl.col('value') > 0)
    )
    if len(null_columns_df) > 0:
        logger.warning(f'found null columns {null_columns_df}')

    games = games.with_columns(
        pl.col('Map').struct.field('scriptName').alias('Map Name'),
        pl.lit([]).alias('Merged Win Replays'),
        pl.lit([]).alias(col.MergedLossReplays),
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
            pl.len().alias('n_games'),
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
    logger.info('merging games')
    # for game in games.filter((pl.col(ai_win_column).eq(False))).iter_rows(named=True):
    for game in games.iter_rows(named=True):
        # for game in games.filter(
        #     (pl.col('id').eq('a035616604a68c9d6815cbe3a2d25e40'))
        # ).iter_rows(named=True):
        win = game[ai_win_column]
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
        if game['commanderbuildersenabled'] == 'disabled':
            _ai_gamesetting_equal_columns = {
                x for x in ai_gamesetting_equal_columns if 'commanderbuilders' not in x
            }
            _ai_gamesetting_higher_harder = {
                x for x in ai_gamesetting_higher_harder if 'commanderbuilders' not in x
            }
            _ai_gamesetting_lower_harder = {
                x for x in ai_gamesetting_lower_harder if 'commanderbuilders' not in x
            }
        if game['assistdronesenabled'] == 'disabled':
            _ai_gamesetting_equal_columns = {
                x for x in ai_gamesetting_equal_columns if 'assistdrones' not in x
            }
            _ai_gamesetting_higher_harder = {
                x for x in ai_gamesetting_higher_harder if 'assistdrones' not in x
            }
            _ai_gamesetting_lower_harder = {
                x for x in ai_gamesetting_lower_harder if 'assistdrones' not in x
            }
        # if game['nuttyb_hp'] is not None:
        #     _ai_gamesetting_higher_harder |= {'nuttyb_hp'}

        merge_games = games.filter(
            pl.col('id').ne(game['id']),
            *[
                (pl.col(x).is_not_null() & pl.col(x).eq(game[x]))
                for x in (_ai_gamesetting_equal_columns | {'Map Name'})
            ],
            *[
                (
                    pl.col(x).is_not_null()
                    & (pl.col(x).le(game[x]) if win else pl.col(x).ge(game[x]))
                )
                for x in _ai_gamesetting_higher_harder
            ],
            *[
                (
                    pl.col(x).is_not_null()
                    & (pl.col(x).ge(game[x]) if win else pl.col(x).le(game[x]))
                )
                for x in _ai_gamesetting_lower_harder
            ],
        )

        if len(merge_games) == 0:
            # logger.info(f'no easier games for {game["id"]}')
            continue

        games = games.update(
            merge_games.with_columns(
                pl.col(f'Merged {'Win' if win else 'Loss'} Replays')
                .list.concat(pl.lit([game['id']]))
                .alias(f'Merged {'Win' if win else 'Loss'} Replays'),
                winners_extended=pl.col('winners').list.concat(
                    pl.lit(game['winners'] if win else [])
                ),
                players_extended=pl.col('players').list.concat(pl.lit(game['players'])),
            ).select(
                'id',
                'winners_extended',
                'players_extended',
                'Merged Win Replays',
                'Merged Loss Replays',
            ),
            on='id',
        )

    def teammates_weighted_success_rate(in_struct):
        logger.info(f'weighting {len(in_struct)} teammate success rates')

        player_names = [
            winner
            for gamesetting in in_struct
            for winners in gamesetting['games_winners']
            for winner in winners
        ]

        gamesetting_result_row_items = []
        for gamesetting in in_struct:
            success_rate_goal = gamesetting['Success Rate']
            winners_completion = {}
            for winner_name in player_names:
                winner_item = {
                    'teammates_completion': 1.0,
                    'teammates': set(),
                }
                for game_teammates in gamesetting['games_winners']:
                    if winner_item['teammates_completion'] <= success_rate_goal:
                        break
                    game_teammates = set(game_teammates)
                    if winner_name not in game_teammates:
                        continue

                    new_teammates = (
                        game_teammates - winner_item['teammates'] - {winner_name}
                    )
                    winner_item['teammates'] |= new_teammates
                    lobby_size = len(game_teammates)
                    n_new_teammates_and_me = len(new_teammates) + 1
                    teammates_completion_subtract = (1 - success_rate_goal) / (
                        max(
                            1,
                            teammates_coef_a * (lobby_size**2)
                            + teammates_coef_b * lobby_size
                            + teammates_coef_c,
                        )
                        / n_new_teammates_and_me
                    )
                    # if winner_name == '' and success_rate_goal < 0.05:
                    # logger.info(
                    #     f'{success_rate_goal:.2f}: {result_winner['teammates_completion']:.2f} -> {result_winner['teammates_completion'] - teammates_completion_subtract:.2f} new_teammates {n_new_teammates_and_me} lobby {lobby_size}'
                    # )
                    winner_item['teammates_completion'] = max(
                        success_rate_goal,
                        winner_item['teammates_completion']
                        - teammates_completion_subtract,
                    )
                    # if winner_name == '' and success_rate_goal < 0.05:
                    #     s()
                winners_completion[winner_name] = winner_item['teammates_completion']
            gamesetting_result_row_items.append(winners_completion)
        return gamesetting_result_row_items

    logger.info('grouping gamesettings')
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
            .sort_by('startTime', descending=True)
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
            .sort_by('startTime', descending=True)
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

    logger.info('creating correlations')
    group_df = group_df.with_columns(
        group_df.select(group_by_columns)
        .select(cs.numeric() & cs.by_name(*ai_gamesetting_all_columns))
        .transpose()
        .corr()
        .fill_nan(0.5)
        .mean()
        .transpose()
        .to_series()
        .alias('Setting Corr.')
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

    group_df = reorder_tweaks(group_df)

    logger.info('creating pastes')
    pastes = []
    for index, row in enumerate(group_df.iter_rows(named=True)):
        _str = '\n!preset coop\n!draft_mode disabled\n!unit_market 1\n' + (
            f'!map {row["Map Name"]}\n' if row['Map Name'] else ''
        )

        for key, value in row.items():
            value = str(value).strip()
            if (
                key == 'nuttyb_hp'
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
            if row['nuttyb_hp'] is not None
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

    logger.info('creating export df')
    group_export_df = (
        group_df.with_columns(
            pl.col('winners')
            .map_elements(
                lambda _col: ', '.join(_col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            )
            .alias('Winners'),
            pl.col('Players').map_elements(
                lambda _col: ', '.join(_col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Win Replays').map_elements(
                lambda _col: ', '.join(_col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Merged Win Replays').map_elements(
                lambda _col: ', '.join(_col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Loss Replays').map_elements(
                lambda _col: ', '.join(_col.to_list()),
                skip_nulls=True,
                return_dtype=pl.String,
            ),
            pl.col('Merged Loss Replays').map_elements(
                lambda _col: ', '.join(_col.to_list()),
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
                'Merged Loss Replays',
                'Copy Paste',
            ]
            + ['Map Name']
            + list(ai_gamesetting_all_columns)
        )
        .fill_null(' ')
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
                else 1.0,
                return_dtype=pl.Float64,
            )
            .alias('teammates_completion'),
        )
        .group_by('Player')
        .agg(
            (1 - pl.col('Success Rate').min()).alias('Difficulty Record'),
            (1 - pl.col('teammates_completion').min()).alias('Difficulty Completion'),
            pl.col('Setting Combinations').sum(),
            pl.col('Setting Corr.').median(),
        )
        .join(
            basic_player_aggregates,
            on='Player',
            how='left',
            validate='1:1',
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
            pl.col('n_games').clip(0, 20).rank().alias('#Games Rank'),
            pl.col('Setting Combinations').rank().alias('Setting Combinations Rank'),
            pl.col('Win Rate').rank().alias('Win Rate Rank'),
            pl.col('Setting Corr.').rank(descending=True).alias('Setting Corr. Rank'),
        )
        .drop('n_games')
        .with_columns(
            (
                pl.col('Weighted Award Rate Rank') * 1
                # + pl.col('Difficulty Record Rank') * 0.1
                + pl.col('Difficulty Completion Rank') * 0.15
                + pl.col('Setting Combinations Rank') * 0.01
                + pl.col('#Games Rank') * 0.4
                + pl.col('Win Rate Rank') * 0.005
                + pl.col('Setting Corr. Rank') * 0.0001
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
        )
        .sort(by=['PVE Rating', 'Player'], descending=[True, False])
        .fill_nan('')
    )

    pve_rating_players = reorder_column(pve_rating_players, 1, 'Award Rate')
    pve_rating_players = reorder_column(pve_rating_players, 2, 'Weighted Award Rate')
    pve_rating_players = reorder_column(pve_rating_players, 5, '#Games')
    pve_rating_players = reorder_column(pve_rating_players, 6, 'Win Rate')
    pl.Config(tbl_rows=50, tbl_cols=111)

    dump_dict = {
        'pve_ratings': {
            player: rating
            for player, rating in zip(
                *pve_rating_players.select('Player', 'PVE Rating')
                .to_dict(as_series=False)
                .values()
            )
        }
    }
    if dev:
        print(dump_dict)
    else:
        s3 = boto3.client('s3')

        with io.BytesIO() as buffer:
            buffer.write(
                orjson.dumps(
                    dump_dict,
                )
            )
            buffer.seek(0)
            s3.put_object(Bucket='pve-rating-web', Key='pve_ratings.json', Body=buffer)

    logger.info('updating sheets')
    update_sheets(group_export_df, pve_rating_players, prefix=prefix)


def update_sheets(df, rating_number_df, prefix):
    if dev:
        gc = gspread.service_account()
        spreadsheet = gc.open_by_key('1L6MwCR_OWXpd3ujX9mIELbRlNKQrZxjifh4vbF8XBxE')
    else:
        try:
            gc = gspread.service_account_from_dict(orjson.loads(get_secret()))
            spreadsheet = gc.open_by_key('18m3nufi4yZvxatdvgS9SdmGzKN2_YNwg5uKwSHTbDOY')
        except gspread.exceptions.APIError as e:
            logger.exception(e)
            logger.info('failed connection to google, stopping')
            return 'failed'

    worksheet_gamesettings = spreadsheet.worksheet(prefix + ' Gamesettings')

    if not dev and random.randint(1, 10) % 10 == 0:
        logger.info('clearing gamesettings sheet')
        worksheet_gamesettings.clear()
        worksheet_gamesettings.update(
            values=[['UPDATE IN PROGRESS']],
            value_input_option=gspread.utils.ValueInputOption.user_entered,
        )

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

    sheet_id = worksheet_rating_number._properties['sheetId']

    percent_int_cols = [
        rating_number_df.columns.index(x)
        for x in [
            'Difficulty Completion',
            'Difficulty Record',
            'Win Rate',
            'Award Rate',
            'Setting Corr.',
        ]
    ]

    weighted_award_rate_col_index = rating_number_df.columns.index(
        'Weighted Award Rate'
    )
    int_cols = [
        index for index, x in enumerate(rating_number_df.columns) if 'Rank' in x
    ] + [rating_number_df.columns.index(x) for x in ['Setting Combinations']]
    pve_rating_col_index = rating_number_df.columns.index('PVE Rating')
    n_games_col_index = rating_number_df.columns.index('#Games')

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
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startColumnIndex': n_games_col_index,
                            'endColumnIndex': n_games_col_index + 1,
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'RIGHT',
                            }
                        },
                        'fields': 'userEnteredFormat.horizontalAlignment',
                    }
                },
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
                                        'type': 'PERCENT',  # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#numberformattype
                                        'pattern': '#%;#%;',  # https://developers.google.com/sheets/api/guides/formats#number_format_tokens
                                    }
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
                    for col_index in [
                        weighted_award_rate_col_index,
                        pve_rating_col_index,
                    ]
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
                                        'pattern': '#',
                                    }
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
                                'sheetId': sheet_id,
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
                                }
                            },
                            'fields': 'userEnteredFormat.textFormat.fontSize',
                        }
                    },
                ],
            ]
        }
    )


if __name__ == '__main__':
    main()
