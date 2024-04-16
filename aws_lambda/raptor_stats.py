import datetime
import json
import logging
import os
import re
import time
from typing import List

import boto3
import gspread
import polars as pl
import polars.selectors as cs
import pytz
import requests
import s3fs
from cast_frame import cast_frame

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401


logger = logging.getLogger()
logger.setLevel(logging.INFO)

replays_file_name = 'replays.parquet'
replay_details_file_name = 'replays_gamesettings.parquet'
bucket_path = 's3://raptor-stats-parquet/'


def get_df_s3(path):
    s3_path = bucket_path + path
    logger.info(f'fetching from s3 "{path}"')
    _df = pl.read_parquet(s3_path)
    if dev:
        logger.info(f'writing {len(_df)} to {path}')
        _df.to_parquet(path)
    return _df


def get_df(path):
    if dev and not os.path.exists(path):
        return get_df_s3(path)
    path = ('' if dev else bucket_path) + path
    df = pl.read_parquet(path)
    logger.info(f'fetched {len(df)} from "{path}"')
    return df


def store_df(df, path):
    path = path if dev else bucket_path + path
    logger.info(f'writing {len(df)} to {path}')
    if dev:
        df.write_parquet(path)
    else:
        fs = s3fs.S3FileSystem()
        with fs.open(path, mode='wb') as f:
            df.write_parquet(f)


def main():
    games = get_df(replays_file_name)

    n_received_rows = limit = int(os.environ.get('LIMIT', 2 if dev else 20))
    page = 1
    n_total_received_rows = 0
    while n_received_rows == limit and limit > 0 and page <= (1 if dev else 30):
        apiUrl = f'https://api.bar-rts.com/replays?limit={limit}&preset=team&hasBots=true&page={page}'
        if page > 1:
            time.sleep(1.2)
        logger.info(f'fetching {apiUrl}')
        replays_json = requests.get(apiUrl, headers={'User-Agent': 'tetrisface'}).json()

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
        store_df(games, replays_file_name)

    def is_raptors(row):
        for team in row:
            for ai in team['AIs']:
                if ai['shortName'] == 'RaptorsAI':
                    return True
        return False

    def is_scavengers(row):
        for team in row:
            for ai in team['AIs']:
                if ai['shortName'] == 'ScavengersAI':
                    return True
        return False

    def is_draw(row):
        return len(row) <= 1 or all(
            is_win == row[0]['winningTeam']
            for is_win in [team['winningTeam'] for team in row]
        )

    def _winners(row):
        _winners = []
        for team in row:
            if team['winningTeam'] is True:
                if len(team['Players']) > 0:
                    _winners.extend([player['name'] for player in team['Players']])
                elif len(team['AIs']) > 0:
                    _winners.extend([ai['shortName'] for ai in team['AIs']])
        return _winners

    def players(row):
        _players = []
        for team in row:
            _players.extend([player['name'] for player in team['Players']])
        return _players

    games = games.with_columns(
        raptors=pl.col('AllyTeams').map_elements(is_raptors, return_dtype=pl.Boolean),
        scavengers=pl.col('AllyTeams').map_elements(
            is_scavengers, return_dtype=pl.Boolean
        ),
        draw=pl.col('AllyTeams').map_elements(is_draw, return_dtype=pl.Boolean),
        winners=pl.col('AllyTeams').map_elements(_winners, return_dtype=pl.List(str)),
        players=pl.col('AllyTeams').map_elements(players, return_dtype=pl.List(str)),
    )

    games = games.filter(
        (pl.col('raptors') | pl.col('scavengers') & (pl.col('draw') == False))
    )

    games = games.rename({'AllyTeams': 'AllyTeamsList'})

    replay_details_cache = get_df(replay_details_file_name)

    games = games.join(replay_details_cache, how='left', on='id').drop(
        cs.ends_with('_right')
    )
    del replay_details_cache

    def PlayerWinGameFilter(_games):
        return games.filter(
            (
                pl.col('winners')
                .list.set_intersection(['RaptorsAI', 'ScavengersAI'])
                .list.len()
                == 0
            )
            & (pl.col('draw') == False)
            & (pl.col('winners').list.len() > 0)
        )

    previousPlayerWinStartTime = (
        PlayerWinGameFilter(games).select(pl.col('startTime').max()).item()
    )

    def api_replay_detail(_replay_id):
        replay_details = {}
        url = ''
        time.sleep(1.2)
        if _replay_id is not None:
            url = f'https://api.bar-rts.com/replays/{_replay_id}'
            logger.info(f'fetching {url}')
            response = requests.get(url, headers={'User-Agent': 'tetrisface'})
            if response.status_code == 200:
                response_json = response.json()
                replay_details = response_json.get('gameSettings')
                replay_details['awards'] = response_json.get('awards')
                replay_details['AllyTeams'] = response_json.get('AllyTeams')
                replay_details['id'] = _replay_id
                replay_details['fetch_success'] = True
                return replay_details
            else:
                logger.info(f'Failed to fetch data from {url}')
                replay_details['fetch_success'] = False
        return replay_details

    # fetch new
    unfetched = games.filter(pl.col('fetch_success').is_null()).select('id')
    to_fetch_ids = unfetched[: 2 if dev else 30]
    if len(to_fetch_ids) == 0:
        logger.info('no new games to fetch')
        # return
    else:
        logger.info(f'fetching {len(to_fetch_ids)} of {len(unfetched)} missing games')

        fetched = []
        for replay_id in to_fetch_ids.iter_rows():
            fetched.append(api_replay_detail(replay_id[0]))

        games = games.update(pl.DataFrame(fetched, strict=False), how='left', on='id')
    games = cast_frame(games)
    del to_fetch_ids, unfetched

    # del raptors_detail_api
    if not games.filter(pl.col('fetch_success') == False).is_empty():
        logger.info(
            f'failed to fetch {len(games.filter(pl.col('fetch_success') == False))} games'
        )

    # refetch all game details
    # raptor_games["fetch_success"] = False

    # store
    store_df(games, replay_details_file_name)

    # stop without any new wins
    games = PlayerWinGameFilter(games)
    newMaxStartTime, newMaxEndTime = games.select(
        pl.col('startTime'),
        ((pl.col('startTime') + pl.col('durationMs') * 1000000) / 1000)
        .cast(pl.Datetime)
        .alias('newEndTime'),
    ).max()
    tz_stockholm = pytz.timezone('Europe/Stockholm')
    previousStockholm = previousPlayerWinStartTime.replace(tzinfo=pytz.utc).astimezone(
        tz_stockholm
    )
    if (
        previousPlayerWinStartTime
        and newMaxStartTime.item() <= previousPlayerWinStartTime
    ):
        logger.info(f'no new wins since {previousStockholm}')
        if not dev:
            return
    else:
        logger.info(
            f'new wins since {previousStockholm}: {newMaxStartTime.replace(tzinfo=pytz.utc).astimezone(tz_stockholm)}'
        )

    from gamesettings import (
        gamesettings,
        gamesettings_scav,
        hp_multiplier,
        main,
        various,
    )

    possible_tweak_columns = (
        ['tweakunits', 'tweakdefs']
        + [f'tweakunits{i}' for i in range(1, 10)]
        + [f'tweakdefs{i}' for i in range(1, 10)]
    )
    all_nuttyb_tweaks = (
        main + [string for x in hp_multiplier.values() for string in x] + various
    )
    any_nuttyb_tweaks_or_empty = all_nuttyb_tweaks + ['']

    higher_harder = {
        'raptor_spawncountmult',
        'raptor_firstwavesboost',
        # 'raptor_raptorstart', # uncertain
    }
    lower_harder = {
        'startmetal',
        'startenergy',
        'startmetalstorage',
        'startenergystorage',
        'multiplier_builddistance',
        'multiplier_shieldpower',
        'multiplier_buildpower',
        'commanderbuildersrange',
        'commanderbuildersbuildpower',
        'raptor_queentimemult',  # probably harder
        'raptor_spawntimemult',
        'scav_bosstimemult',
        'scav_spawntimemult',
    }

    def gamesettings_mode(row):
        if row['scavengers']:
            ai_gamesettings = gamesettings_scav
            ai_start_setting_name = 'scav_scavstart'
        else:
            ai_gamesettings = gamesettings
            ai_start_setting_name = 'raptor_raptorstart'

        for mode_name, settings in ai_gamesettings.items():
            match = False
            for setting, value in settings.items():
                if row[setting] is not None and (
                    row[setting] == value
                    or (setting in higher_harder and row[setting] >= value)
                    or (setting == ai_start_setting_name and row[setting] == 'avoid')
                    or (setting in lower_harder and row[setting] <= value)
                ):
                    # logger.info(f'value matching mode {mode_name} {setting} {row[setting]} ~= {value} higher harder {setting in higher_harder} lower harder {setting in lower_harder}')
                    match = True
                elif row[setting] != value:
                    # logger.info(
                    #     f'value not matching mode {mode_name} {setting} {row[setting]} != {value}'
                    # )
                    # if row['id'] == '':
                    #     s()
                    match = False
                    break
                else:
                    raise Exception(f'unhandled setting {setting} value {value}')
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
            return '"Missing"'
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
        # if row['id'] == '':
        #     s()
        if (
            row['nuttyb_tweaks_exclusive']
            and row['Gamesettings Mode'] in nuttyb_exclusive_modes
        ):
            return f"NuttyB Default {row['nuttyb_hp']}"
        elif row['nuttyb_main']:
            if row['nuttyb_hp']:
                return f"NuttyB Main & HP {row['nuttyb_hp']}"
            elif row['raptor_difficulty']:
                return f"NuttyB Main & {format_regular_diff(row['raptor_difficulty'])}"
            else:
                return 'NuttyB Main "Missing"'

        elif row['nuttyb_hp']:
            return f'NuttyB HP {row["nuttyb_hp"]}'
        elif row['raptors'] and row['raptor_difficulty']:
            return f'Raptors {format_regular_diff(row["raptor_difficulty"])}'
        elif row['scavengers'] and row['scav_difficulty']:
            return f'Scavengers {format_regular_diff(row["scav_difficulty"])}'
        logger.warning(
            'no diff found for',
            row[
                [
                    'nuttyb_main',
                    'nuttyb_hp',
                    'nuttyb_tweaks_exclusive',
                    'raptor_difficulty',
                    'scav_difficulty',
                    'raptors',
                    'scavengers',
                ]
            ],
        )
        return ''

    games = games.with_columns(
        nuttyb_main=pl.concat_list(possible_tweak_columns)
        .list.set_intersection(main)
        .list.len()
        > 0,
        nuttyb_hp=pl.when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(hp_multiplier['Epicest'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epicest'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(hp_multiplier['Epicer+'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epicer+'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(hp_multiplier['Epic++'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epic++'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(hp_multiplier['Epic+'])
            .list.len()
            > 0
        )
        .then(pl.lit('Epic+'))
        .when(
            pl.concat_list(possible_tweak_columns)
            .list.set_intersection(hp_multiplier['Epic'])
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

    difficulty_enum = pl.Enum(
        [
            'NuttyB Default Epicest',
            'NuttyB Default Epicer+',
            'NuttyB Default Epic++',
            'NuttyB Default Epic+',
            'NuttyB Default Epic',
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
            'NuttyB Main "Missing"',
            'Raptors Epic',
            'Raptors Very Hard',
            'Raptors Hard',
            'Raptors Normal',
            'Raptors Easy',
            'Raptors Very Easy',
            'Raptors "Missing"',
            'Scavengers Epic',
            'Scavengers Very Hard',
            'Scavengers Hard',
            'Scavengers Normal',
            'Scavengers Easy',
            'Scavengers Very Easy',
            'Scavengers "Missing"',
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
        .filter(
            pl.col('Player').list.set_intersection(pl.col('winners')).list.len() == 1
        )
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
    else:
        gc = gspread.service_account_from_dict(json.loads(get_secret()))

    spreadsheet = (
        gc.open_by_key('1w8Ng9GGUo6DU0rBFRYRnC_JxK8nHTSSjf1Nbq1e-3bc')
        if dev
        else gc.open_by_key('1oI7EJIUiwLLXDMBgky2BN8gM6eaQRb9poWGiP2IKot0')
    )

    update_sheet(spreadsheet, grouped(players), ' All', newMaxEndTime)
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


def get_secret():
    secret_name = 'raptor-gcp'
    region_name = 'eu-north-1'

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise e

    return get_secret_value_response['SecretString']


def links_cell(url_pairs):
    cell = f'=ifna(hyperlink("{url_pairs[0][0]}";"[{url_pairs[0][1]+1}]")'
    if len(url_pairs) > 1:
        cell += (
            '; "'
            + ', '.join([f'[{index+1}] {url}' for url, index in url_pairs[1:]])
            + '"'
        )
    return cell + ')'


def grouped(to_group_df):
    if len(to_group_df) == 0:
        return to_group_df
    all_groups = []
    logger.info(f'grouping {len(to_group_df)} rows')

    for index, ((group_diff, group_mode), group_df) in enumerate(
        to_group_df.groupby(
            ['Difficulty', 'Gamesettings Mode'],
            maintain_order=True,
        )
    ):
        group_name = str(group_diff) + (
            ' - ' + group_mode if group_mode and group_mode is not None else ''
        )

        group_players = (
            group_df.groupby(['Player'])
            .agg(
                pl.col('Player').count().alias('n_victories'),
                (pl.sum('🏆DMG') + pl.sum('🏆ECO')).alias('awards_sum'),
                pl.sum('🏆DMG').str.replace(r'^0$', ''),
                pl.sum('🏆ECO').str.replace(r'^0$', ''),
                pl.col('id')
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
                pl.min('durationMs').alias('min_duration'),
                pl.duration(minutes=pl.col('durationMs').min() / 1000 / 60).alias(
                    '⏱Time'
                ),
            )
            .with_columns(
                pl.col('Player').str.to_lowercase().alias('player'),
            )
            .sort(
                [
                    'n_victories',
                    'awards_sum',
                    '🏆DMG',
                    'min_duration',
                    'player',
                ],
                descending=[
                    True,
                    True,
                    True,
                    False,
                    False,
                ],
            )
            .select(
                [
                    'Player',
                    'Victories',
                    '🏆DMG',
                    '🏆ECO',
                    '⏱Time',
                ]
            )
        )

        all_groups.append(
            [
                group_name,
                group_players
                if index == 0
                else group_players.select(pl.all().map_alias(lambda x: f'{x}_{index}')),
            ]
        )

    return all_groups


def update_sheet(spreadsheet, groups, sheet_name_postfix, last_win):
    if len(groups) == 0:
        return

    top_header, data_groups = zip(*groups)

    data_columns = data_groups[0].columns

    len_data_columns = len(data_columns)

    second_header = data_columns * len(data_groups)

    top_header = list(top_header)
    top_header_expanded = [''] * int(
        (len_data_columns - 1) / len_data_columns * len(second_header)
    )
    for i in range(0, len(second_header)):
        if i % len_data_columns == 0:
            top_header_expanded.insert(i, top_header.pop(0))

    # rows = pl.concat(data_groups, how='horizontal').rows()
    rows = []
    for row in pl.concat(data_groups, how='horizontal').rows():
        cells = []
        for cell in row:
            if isinstance(cell, (pl.Series, List)):
                cell = cell[0]
            if isinstance(cell, datetime.timedelta):
                seconds = cell.total_seconds()
                hours, remainder = divmod(seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                cell = re.sub(
                    r'^0h |(^|(?<=\s))0m |(^|(?<=\s))0s',
                    '',
                    f'{int(hours)}h {int(minutes)}m {int(seconds)}s',
                )
            cells.append(cell)
        rows.append(cells)

    values = [top_header_expanded] + [second_header] + rows

    sheet_rows = len(rows) + 2
    len_second_header = len(second_header)

    sheet_name = datetime.date.today().strftime('%Y-%m') + sheet_name_postfix
    logger.info(
        f"updating sheet '{sheet_name}', {sheet_rows} rows, {len_second_header} cols"
    )
    new_sheet = False
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=sheet_rows,
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
