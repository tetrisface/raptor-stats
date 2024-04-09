import pandas as pd
import requests
import os
import time
import logging
import warnings
import json

from warnings import simplefilter

simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
logger = logging.getLogger()

pd.options.mode.copy_on_write = True


def main():

    games = pd.DataFrame()
    bucket_name = os.environ.get("BUCKET_NAME", False)
    logger.info(f'bucket_name: {bucket_name}')
    if bucket_name:
        file_path = f"s3://{bucket_name}/replays.parquet"
    else:
        file_path = "replays.parquet"

    if bucket_name or os.path.exists(file_path):
        disk = pd.read_parquet(file_path)
        if "id" in disk.columns:
            disk = disk.set_index("id")
        games = disk

    n_api_new_rows = limit = 20
    page = 0
    while n_api_new_rows == limit and limit > 0 and page <= 3:
        page += 1
        apiUrl = f"https://api.bar-rts.com/replays?limit={limit}&preset=team&hasBots=true&page={page}"
        if page > 1:
            time.sleep(1.2)
        logger.info(f'fetching {apiUrl}')
        replays_json = requests.get(apiUrl, headers={"User-Agent": "tetrisface"}).json()

        data = replays_json["data"]

        api = pd.DataFrame.from_records(data).set_index("id")

        api_new_indices = api.index.difference(games.index)
        n_api_new_rows = len(api_new_indices)
        n_before_games = len(games)
        games = pd.concat(
            [games, api.loc[api_new_indices]],
            verify_integrity=True,
            axis=0,
        )
        games.startTime = pd.to_datetime(games.startTime)
        games.durationMs = pd.to_numeric(games.durationMs, downcast="integer")
        games.drop(["Map"], axis=1, errors="ignore", inplace=True)
        logger.info(f"games {n_before_games} + {n_api_new_rows} = {len(games)}")
    games.to_parquet(file_path)


    def is_raptors(row):
        for team in row:
            for ai in team["AIs"]:
                if ai["shortName"] == "RaptorsAI":
                    return True
        return False


    def is_draw(row):
        results = []
        for team in row:
            results.append(team["winningTeam"])
        return len(team) <= 1 or all(
            x == results[0] for x in [team["winningTeam"] for team in row]
        )


    def all_winners(row):
        _winners = []
        for team in row:
            if team["winningTeam"] is True:
                if len(team["Players"]) > 0:
                    _winners.extend([player["name"] for player in team["Players"]])
                elif len(team["AIs"]) > 0:
                    _winners.extend([ai["shortName"] for ai in team["AIs"]])
        return _winners


    def players(row):
        _players = []
        for team in row:
            _players.extend([player["name"] for player in team["Players"]])
        return _players


    games["raptors"] = games["AllyTeams"].apply(is_raptors)
    games["draw"] = games["AllyTeams"].apply(is_draw)
    games["winners"] = games["AllyTeams"].apply(all_winners)
    games["players"] = games["AllyTeams"].apply(players)





    numerical_columns = [
        "ai_incomemultiplier",
        "air_rework",
        "allowpausegameplay",
        "allowuserwidgets",
        "april1",
        "april1extra",
        "assistdronesair",
        "assistdronesbuildpowermultiplier",
        "assistdronescount",
        "capturebonus",
        "captureradius",
        "capturetime",
        "commanderbuildersbuildpower",
        "commanderbuildersrange",
        "coop",
        "critters",
        "debugcommands",
        "decapspeed",
        "defaultdecals",
        "disable_fogofwar",
        "disablemapdamage",
        "dominationscore",
        "dominationscoretime",
        "easter_egg_hunt",
        "easteregghunt",
        "emprework",
        "energyperpoint",
        "experimentalextraunits",
        "experimentalimprovedtransports",
        "experimentallegionfaction",
        "experimentalmassoverride",
        "experimentalnoaircollisions",
        "experimentalrebalancet2energy",
        "experimentalrebalancet2labs",
        "experimentalrebalancet2metalextractors",
        "experimentalrebalancewreckstandarization",
        "experimentalreversegear",
        "experimentalxpgain",
        "faction_limiter",
        "ffa_wreckage",
        "fixedallies",
        "lategame_rebalance",
        "limitscore",
        "map_atmosphere",
        "map_waterislava",
        "map_waterlevel",
        "maxunits",
        "metalperpoint",
        "multiplier_builddistance",
        "multiplier_buildpower",
        "multiplier_buildtimecost",
        "multiplier_energyconversion",
        "multiplier_energycost",
        "multiplier_energyproduction",
        "multiplier_losrange",
        "multiplier_maxdamage",
        "multiplier_maxvelocity",
        "multiplier_metalcost",
        "multiplier_metalextraction",
        "multiplier_radarrange",
        "multiplier_resourceincome",
        "multiplier_shieldpower",
        "multiplier_turnrate",
        "multiplier_weapondamage",
        "multiplier_weaponrange",
        "norush",
        "norushtimer",
        "numberofcontrolpoints",
        "proposed_unit_reworks",
        "ranked_game",
        "raptor_endless",
        "raptor_firstwavesboost",
        "raptor_graceperiodmult",
        "raptor_queentimemult",
        "raptor_spawncountmult",
        "raptor_spawntimemult",
        "releasecandidates",
        "ruins_civilian_disable",
        "ruins_only_t1",
        "scav_bosstimemult",
        "scav_endless",
        "scav_graceperiodmult",
        "scav_spawncountmult",
        "scav_spawntimemult",
        "scoremode_chess_adduptime",
        "scoremode_chess_spawnsperphase",
        "scoremode_chess_unbalanced",
        "scoremode_chess",
        "shareddynamicalliancevictory",
        "skyshift",
        "startenergy",
        "startenergystorage",
        "startmetal",
        "startmetalstorage",
        "starttime",
        "teamffa_start_boxes_shuffle",
        "tugofwarmodifier",
        "unified_maxslope",
        "unit_restrictions_noair",
        "unit_restrictions_noconverters",
        "unit_restrictions_noendgamelrpc",
        "unit_restrictions_noextractors",
        "unit_restrictions_nolrpc",
        "unit_restrictions_nonukes",
        "unit_restrictions_notacnukes",
        "unit_restrictions_notech2",
        "unit_restrictions_notech3",
        "usemapconfig",
        "usemexconfig",
    ]
    string_columns = [
        "assistdronesenabled",
        "commanderbuildersenabled",
        "deathmode",
        "experimentalshields",
        "experimentalstandardgravity",
        "lootboxes_density",
        "lootboxes",
        "map_tidal",
        "raptor_difficulty",
        "raptor_raptorstart",
        "ruins_density",
        "ruins",
        "scav_difficulty",
        "scav_scavstart",
        "scoremode",
        "teamcolors_anonymous_mode",
        "teamcolors_icon_dev_mode",
        "transportenemy",
        "tweakdefs",
        "tweakdefs1",
        "tweakdefs2",
        "tweakdefs3",
        "tweakdefs4",
        "tweakdefs5",
        "tweakdefs6",
        "tweakdefs7",
        "tweakdefs8",
        "tweakdefs9",
        "tweakunits",
        "tweakunits1",
        "tweakunits2",
        "tweakunits3",
        "tweakunits4",
        "tweakunits5",
        "tweakunits6",
        "tweakunits7",
        "tweakunits8",
        "tweakunits9",
    ]


    def cast_frame(_df):

        for col in string_columns:
            _df[string_columns] = _df[string_columns].fillna("")

        _df = _df.astype({col: str for col in string_columns}, errors="raise")

        for col in numerical_columns:
            _df[numerical_columns] = _df[numerical_columns].fillna(0)

        for col in numerical_columns:
            _df[col] = pd.to_numeric(
                _df[col],
                downcast="integer",
            )

        return _df





    raptor_games = games[
        games["raptors"]
        # & ~df_root_expanded["draw"]
    ]  # draws might be good to exclude

    raptor_games["fetch_success"] = None


    def api_replay_detail(row):
        time.sleep(1.2)
        if row is not None and row.name is not None:
            url = f"https://api.bar-rts.com/replays/{row.name}"
            response = requests.get(url, headers={"User-Agent": "tetrisface"})
            if response.status_code == 200:
                response_json = response.json()
                replay_details = response_json.get("gameSettings")
                replay_details["awards"] = response_json.get("awards")
                replay_details["AllyTeams"] = response_json.get("AllyTeams")
                for key, value in replay_details.items():
                    # Add new column to DataFrame if the column doesn't exist
                    if key not in raptor_games.columns:
                        raptor_games[key] = None
                    # Update DataFrame with fetched data
                    row[key] = value
                row["fetch_success"] = True
                return row
        logger.info(f"Failed to fetch data from {url}")
        row["fetch_success"] = False
        return row


    raptor_games.durationMs = raptor_games.durationMs.astype("int64")

    # load from cache
    bucket_name = os.environ.get("BUCKET_NAME", False)
    if bucket_name:
        game_detail_path = f"s3://{bucket_name}/replays_gamesettings.parquet"
    else:
        game_detail_path = "replays_gamesettings.parquet"

    if bucket_name or os.path.exists(game_detail_path):
        disk = pd.read_parquet(game_detail_path)
        raptor_games.loc[disk.index, disk.columns] = disk

    raptor_games['raptor_win'] = raptor_games.apply(lambda row: 'RaptorsAI' in row['winners'] and (row['draw'] == False), axis=1)
    raptor_games['player_win'] = raptor_games.apply(lambda row: ('RaptorsAI' not in row['winners'])
                                                    and (len(row['winners']) > 0)
                                                    and (row['draw'] == False), axis=1)

    previousPlayerWinStartTime = raptor_games[raptor_games['player_win']].startTime.max()

    isnull = raptor_games[
        raptor_games.fetch_success.isnull() | (raptor_games.fetch_success == False)
    ]
    to_fetch = isnull.head(100)
    logger.info(f"fetching {len(to_fetch)} of {len(isnull)} missing games")

    # fetch new
    df_raptors_api = to_fetch.apply(
        api_replay_detail,
        axis=1,
    )
    raptor_games.loc[df_raptors_api.index, df_raptors_api.columns] = cast_frame(
        df_raptors_api
    )

    if len(raptor_games.loc[raptor_games.fetch_success == False]) > 0:
        logger.info(f"failed to fetch {len(raptor_games[~raptor_games.fetch_success])} games")

    raptor_games = cast_frame(raptor_games)

    # raptor_games.info(verbose=True)

    # refetch all game details
    # raptor_games["fetch_success"] = False

    # store
    raptor_games[raptor_games["fetch_success"].notnull()].to_parquet(game_detail_path)

    raptor_games['raptor_win'] = raptor_games.apply(lambda row: 'RaptorsAI' in row['winners'] and (row['draw'] == False), axis=1)
    raptor_games['player_win'] = raptor_games.apply(lambda row: ('RaptorsAI' not in row['winners'])
                                                    and (len(row['winners']) > 0)
                                                    and (row['draw'] == False), axis=1)

    # stop without any new wins
    newWinsStartTime = raptor_games[raptor_games['raptor_win']].startTime.max()
    if previousPlayerWinStartTime and newWinsStartTime <= previousPlayerWinStartTime:
        logger.info(f"no new wins since {previousPlayerWinStartTime}")
        return
    else:
        logger.info(f"new wins since {previousPlayerWinStartTime}: {newWinsStartTime}")

    from gamesettings import hp_multiplier, main, coms, meganuke_149, wind_restrict_149, gamesettings

    raptor_games["nuttyb_main"] = raptor_games.apply(
        lambda row: all(
            [
                tweak["value"] == row[tweak["location"]]
                for tweak in main
                if tweak["version"] == "1.48"
            ]
        )
        or all(
            [
                tweak["value"] == row[tweak["location"]]
                for tweak in main
                if tweak["version"] == "1.49"
            ]
        ),
        axis=1,
    )


    def nuttyb_difficulty(row):
        for _def in hp_multiplier:
            if 'values' in _def:
                if row[_def["location"]] in _def["values"]:
                    return _def["name"]
            else:
                if row[_def["location"]] == _def["value"]:
                    return _def["name"]
        return None


    raptor_games["nuttyb_hp"] = raptor_games.apply(nuttyb_difficulty, axis=1)


    possible_tweaks = ['tweakunits', 'tweakdefs'] +[f'tweakunits{i}' for i in range(1, 10)] + [f'tweakdefs{i}' for i in range(1, 10)]
    all_allowed_tweaks = []
    for setting_dict in [setting_dict for setting_dict in [*main, *hp_multiplier, *coms, meganuke_149, wind_restrict_149]]:
        if 'values' in setting_dict:
            all_allowed_tweaks.extend(setting_dict['values'])
        else:
            all_allowed_tweaks.append(setting_dict['value'])

    def is_nuttyb_tweaks_exclusive(row):
        if row["nuttyb_main"] == True \
            and row["nuttyb_hp"] is not None and len(row["nuttyb_hp"]) > 0 \
            and row[possible_tweaks][row[possible_tweaks].astype(bool)].isin(all_allowed_tweaks).all():
            return True
        return False

    raptor_games["nuttyb_tweaks_exclusive"] = raptor_games.apply(is_nuttyb_tweaks_exclusive, axis=1)


    raptor_games['raptor_raptorstart'] = pd.Categorical(
        raptor_games['raptor_raptorstart'],
        ['alwaysbox', 'initialbox', 'avoid'],
        ordered=True
    )
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
        'raptor_queentimemult', # probably harder
        'raptor_spawntimemult',
    }

    def nuttyb_mode(row):
        for mode_name, settings in gamesettings.items():
            match = False
            for setting, value in settings.items():
                if row[setting] == value or (setting in higher_harder and row[setting] >= value) or (setting == 'raptor_raptorstart' and row[setting] == 'avoid') or (setting in lower_harder and row[setting] <= value):
                    # logger.info(f'value matching mode {mode_name} {setting} {row[setting]} ~= {value} higher harder {setting in higher_harder} lower harder {setting in lower_harder}')
                    match = True
                elif row[setting] != value:
                    # logger.info(f'value not matching mode {mode_name} {setting} {row[setting]} != {value}')
                    match = False
                    break
                else:
                    raise Exception(f"unhandled setting {setting} value {value}")
            if match:
                return mode_name
        return ''

    # test replay mode
    # logger.info(f'found mode { nuttyb_mode(raptor_games.loc['5bd8116605ce75f19618af055dd363a4']) }')
    # logger.info(f'found mode { nuttyb_mode(raptor_games.loc['46990c66b597eee2b2d25216620d652b']) }')
    # logger.info(f'found mode { nuttyb_mode(raptor_games.loc['ee33136637b2f15c3e72fbcf2585e796']) }')
    raptor_games["Gamesettings Mode"] = pd.Categorical(
        raptor_games.apply(nuttyb_mode, axis=1),
        [
            "Gauntlet",
            "0 grace zerg",
            "Zerg",
            "0 grace",
            "Rush",
            "Gauntlet 1.48",
            "Zerg 1.48",
            "Rush 1.48",
            "Max spawn, 1k res",
            "Max spawn, 5k res",
            "Max spawn, 10k res",
            "Max spawn, 1k res, extraunits",
            "Max spawn, 5k res, extraunits",
            "Max spawn, 10k res, extraunits",
            '',
        ],
        ordered=True
    )


    def format_raptor_diff(string):
        return string.replace('very','very ').title()

    def raptor_diff(row):
        if row["nuttyb_tweaks_exclusive"] and row['Gamesettings Mode'] in {"Gauntlet", "0 grace zerg", "Zerg", "0 grace", "Rush", "Gauntlet 1.48", "Zerg 1.48", "Rush 1.48"}:
            return f"NuttyB Default {row['nuttyb_hp']}"
        elif row["nuttyb_main"]:
            if row["nuttyb_hp"]:
                return f"NuttyB Main & HP {row["nuttyb_hp"]}"
            else:
                return f"NuttyB Main & {format_raptor_diff(row["raptor_difficulty"])}"
        elif row["nuttyb_hp"]:
            return f"NuttyB HP {row["nuttyb_hp"]}"
        else:
            return f"{format_raptor_diff(row["raptor_difficulty"])}"
        logger.info('no diff found for', row[["nuttyb_main", "nuttyb_hp", "raptor_difficulty", "nuttyb_tweaks_exclusive"]])
        return ''

    raptor_games["Difficulty"] = pd.Categorical(
        raptor_games.apply(
            raptor_diff,
            axis=1),
            [
                "NuttyB Default Epicest",
                "NuttyB Default Epicer+",
                "NuttyB Default Epic++",
                "NuttyB Default Epic+",
                "NuttyB Default Epic",
                "NuttyB Main & HP Epicest",
                "NuttyB Main & HP Epicer+",
                "NuttyB Main & HP Epic++",
                "NuttyB Main & HP Epic+",
                "NuttyB Main & HP Epic",
                "NuttyB HP Epicest",
                "NuttyB HP Epicer+",
                "NuttyB HP Epic++",
                "NuttyB HP Epic+",
                "NuttyB HP Epic",
                "NuttyB Main & Epic",
                "NuttyB Main & Very Hard",
                "NuttyB Main & Hard",
                "NuttyB Main & Normal",
                "NuttyB Main & Easy",
                "NuttyB Main & Very Easy",
                "Epic",
                "Very Hard",
                "Hard",
                "Normal",
                "Easy",
                "Very Easy",
                '',
            ],
            ordered=True,
    )

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        try:
            na_games = raptor_games[raptor_games["Difficulty"].isna() & raptor_games["fetch_success"] == True]
            na_games_related = na_games[['nuttyb_main', "nuttyb_hp",'raptor_difficulty', 'Difficulty', 'Gamesettings Mode']]
            assert (
                len(na_games) == 0
            ), f'missing difficulties for {len(na_games)} {na_games_related}'
        except AssertionError as e:
            logger.info(e)
            logger.info(na_games_related)





    def awards(row):
        player_team_id = None
        for ally_team in row["AllyTeams"]:
            if len(ally_team["Players"]) > 0:
                for player in ally_team["Players"]:
                    if player["name"] == row["player"] and "teamId" in player:
                        player_team_id = player["teamId"]
                        break

        if player_team_id is None or not row["awards"]:
            return pd.Series([0, 0])

        damage = 0
        eco = 0
        try:
            if player_team_id == row["awards"]["fightingUnitsDestroyed"][0]["teamId"]:
                damage = 1
        except KeyError:
            damage = 0

        try:
            if player_team_id == row["awards"]["mostResourcesProduced"]["teamId"]:
                eco = 1
        except KeyError:
            eco = 0

        return pd.Series([damage, eco])


    def links_cell(url_pairs):
        cell = "=" + f'ifna(hyperlink("{url_pairs[0][0]}";"[{url_pairs[0][1]+1}]")'
        if len(url_pairs) > 1:
            cell += (
                '; "'
                + ", ".join([f"[{index+1}] {url}" for url, index in url_pairs[1:]])
                + '"'
            )
        return cell + ")"


    def grouped(_df):
        all_groups = pd.DataFrame()
        for (group_diff, group_mode), group_df in _df.groupby(
            ["Difficulty", "Gamesettings Mode"],
            observed=True,
            sort=True,
            dropna=False,
        ):
            group_df["_player_group_bypass"] = group_df["player"]
            group_df["_player_lower_case"] = group_df["player"].str.lower()
            group_df["game_player_n_awards"] = (
                group_df["award_damage"] + group_df["award_eco"]
            )
            group_df["Replays"] = group_df.index

            group_players = (
                group_df.groupby(["player"])
                .agg(
                    {
                        "player": "count",
                        "game_player_n_awards": "sum",
                        "award_damage": "sum",
                        "award_eco": "sum",
                        "Replays": lambda replay_ids: links_cell(
                            [
                                (f"https://bar-rts.com/replays/{replay_id}", index)
                                for index, replay_id in list(
                                    reversed(list(enumerate(replay_ids)))
                                )
                            ]
                        ),
                        "_player_group_bypass": lambda x: x.iloc[0],
                        "_player_lower_case": lambda x: x.iloc[0],
                    }
                )
                .rename(
                    columns={
                        "player": "_victories_count",
                        "_player_group_bypass": "Player",
                        "Replays": "Victories",
                        "award_damage": "🏆DMG",
                        "award_eco": "🏆ECO",
                    },
                )
                .reset_index(drop=True)
                .sort_values(
                    [
                        "_victories_count",
                        "game_player_n_awards",
                        "🏆DMG",
                        "_player_lower_case",
                    ],
                    ascending=[
                        False,
                        False,
                        False,
                        True,
                    ],
                )
                .reset_index(drop=True)[["Player", "Victories", "🏆DMG", "🏆ECO"]]
            )
            pd.set_option(
                "display.max_columns",
                None,
                "display.max_rows",
                None,
            )
            group_players.columns = pd.MultiIndex.from_tuples(
                [
                    (
                        group_diff
                        + (
                            " - " + group_mode
                            if group_mode and not pd.isna(group_mode)
                            else ""
                        ),
                        second_level_columns,
                    )
                    for second_level_columns in group_players.columns
                ]
            )

            all_groups = (
                pd.concat(
                    [
                        all_groups,
                        group_players,
                    ],
                    axis=1,
                )
                .fillna("")
                .replace(0, "")
            )

        return all_groups


    import datetime
    import gspread
    import boto3

    def get_secret():

        secret_name = "raptor-gcp"
        region_name = "eu-north-1"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e


        return get_secret_value_response['SecretString']

    gc = gspread.service_account_from_dict(json.loads(get_secret()))

    spreadsheet = gc.open_by_key("1oI7EJIUiwLLXDMBgky2BN8gM6eaQRb9poWGiP2IKot0")
    # spreadsheet = gc.open_by_key("1w8Ng9GGUo6DU0rBFRYRnC_JxK8nHTSSjf1Nbq1e-3bc")  # dev



    def update_sheet(_df, sheet_name_postfix):
        sheet_name = datetime.date.today().strftime("%Y-%m") + sheet_name_postfix
        logger.info(f"updating sheet {sheet_name}")
        new_sheet = False
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, rows=len(_df), cols=len(_df.columns), index=0
            )
            new_sheet = True

        first_header, second_header = zip(*_df.columns)
        values = (
            [[x if index % 4 == 0 else "" for index, x in enumerate(first_header)]]
            + [second_header]
            + _df.values.tolist()
        )
        worksheet.update(
            values=values,
            value_input_option=gspread.utils.ValueInputOption.user_entered,
        )

        unicode_char_columns = [index for index, x in enumerate(second_header) if "🏆" in x]
        player_columns = [index for index, x in enumerate(second_header) if "Player" in x]

        sheet_id = worksheet._properties["sheetId"]
        body = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "fontSize": 11,
                                },
                                "hyperlinkDisplayType": "LINKED",
                            }
                        },
                        "fields": "userEnteredFormat",
                    }
                },
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                        }
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "fontSize": 10,
                                },
                                "hyperlinkDisplayType": "LINKED",
                            }
                        },
                        "fields": "userEnteredFormat",
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"frozenRowCount": 2},
                        },
                        "fields": "gridProperties(frozenRowCount)",
                    }
                },
            ]
            + [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": index,
                            "endIndex": index + 1,
                        },
                        "properties": {"pixelSize": 52},
                        "fields": "pixelSize",
                    }
                }
                for index in unicode_char_columns
            ]
            + [
                {
                    "repeatCell": {
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "LEFT",
                            }
                        },
                        "range": {
                            "sheetId": sheet_id,
                            "startColumnIndex": index,
                            "endColumnIndex": index + 1,
                        },
                        "fields": "userEnteredFormat(horizontalAlignment)",
                    }
                }
                for index in player_columns
            ]
        }
        if new_sheet:
            body["requests"].append(
                {
                    "addBanding": {
                        "bandedRange": {
                            "bandedRangeId": sheet_id,
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 2,
                            },
                            "rowProperties": {
                                "firstBandColorStyle": {
                                    "rgbColor": {
                                        "red": 243 / 255,
                                        "green": 243 / 255,
                                        "blue": 243 / 255,
                                    },
                                },
                                "secondBandColorStyle": {"themeColor": "BACKGROUND"},
                            },
                        },
                    },
                }
            )
        res = spreadsheet.batch_update(body)


    raptor_players = raptor_games.explode("players")
    raptor_players.rename(columns={"players": "player"}, inplace=True)

    raptor_players["won"] = raptor_players.apply(
        lambda row: row.player in row.winners, axis=1
    )

    raptor_players[["award_damage", "award_eco"]] = raptor_players.apply(awards, axis=1)

    all_winners = raptor_players[raptor_players["player_win"]]

    for game_type_winners, postfix in [
        (all_winners, " All"),
        (all_winners[all_winners["Difficulty"].isin(["Epic", "Very Hard", "Hard", "Normal", "Easy", "Very Easy"])], " Regular"),
        (all_winners[all_winners["nuttyb_tweaks_exclusive"] == True], " NuttyB")
        ]:
        update_sheet(grouped(game_type_winners), postfix)

