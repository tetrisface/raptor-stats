import logging
import os

import polars as pl
from Common.gamesettings import nuttyb_hp_multiplier, possible_tweak_columns

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401


string_columns = {
    'assistdronesenabled',
    'commanderbuildersenabled',
    'deathmode',
    'draft_mode',
    'experimentalshields',
    'experimentalstandardgravity',
    'lootboxes',
    'lootboxes_density',
    'map_tidal',
    'raptor_raptorstart',
    'ruins_density',
    'ruins',
    'scav_scavstart',
    'scoremode',
    'teamcolors_anonymous_mode',
    'teamcolors_icon_dev_mode',
    'transportenemy',
    'tweakunits',
    'tweakunits1',
    'tweakunits2',
    'tweakunits3',
    'tweakunits4',
    'tweakunits5',
    'tweakunits6',
    'tweakunits7',
    'tweakunits8',
    'tweakunits9',
    'tweakdefs',
    'tweakdefs1',
    'tweakdefs2',
    'tweakdefs3',
    'tweakdefs4',
    'tweakdefs5',
    'tweakdefs6',
    'tweakdefs7',
    'tweakdefs8',
    'tweakdefs9',
}
numerical_columns = {
    'ai_incomemultiplier',
    'air_rework',
    'allowpausegameplay',
    'allowuserwidgets',
    'april1',
    'april1extra',
    'assistdronesair',
    'assistdronesbuildpowermultiplier',
    'assistdronescount',
    'capturebonus',
    'captureradius',
    'capturetime',
    'commanderbuildersbuildpower',
    'commanderbuildersrange',
    'coop',
    'critters',
    'debugcommands',
    'decapspeed',
    'defaultdecals',
    'disable_fogofwar',
    'disablemapdamage',
    'dominationscore',
    'dominationscoretime',
    'durationMs',
    'easter_egg_hunt',
    'easteregghunt',
    'emprework',
    'energy_share_rework',
    'energyperpoint',
    'evocom',
    'evocomxpmultiplier',
    'experimentalextraunits',
    'experimentalimprovedtransports',
    'experimentallegionfaction',
    'experimentalmassoverride',
    'experimentalnoaircollisions',
    'experimentalrebalancet2energy',
    'experimentalrebalancet2labs',
    'experimentalrebalancet2metalextractors',
    'experimentalrebalancewreckstandarization',
    'experimentalreversegear',
    'experimentalxpgain',
    'faction_limiter',
    'ffa_wreckage',
    'fixedallies',
    'forceallunits',
    'fullDurationMs',
    'junorework',
    'lategame_rebalance',
    'limitscore',
    'map_atmosphere',
    'map_waterislava',
    'map_waterlevel',
    'maxunits',
    'metalperpoint',
    'norush',
    'norushtimer',
    'numberofcontrolpoints',
    'proposed_unit_reworks',
    'ranked_game',
    'raptor_endless',
    'raptor_firstwavesboost',
    'raptor_graceperiodmult',
    'raptor_queentimemult',
    'raptor_spawncountmult',
    'raptor_spawntimemult',
    'releasecandidates',
    'ruins_civilian_disable',
    'ruins_only_t1',
    'scav_bosstimemult',
    'scav_endless',
    'scav_graceperiodmult',
    'scav_spawncountmult',
    'scav_spawntimemult',
    'scoremode_chess_adduptime',
    'scoremode_chess_spawnsperphase',
    'scoremode_chess_unbalanced',
    'scoremode_chess',
    'shareddynamicalliancevictory',
    'skyshift',
    'startenergy',
    'startenergystorage',
    'startmetal',
    'startmetalstorage',
    'starttime',
    'teamffa_start_boxes_shuffle',
    'tugofwarmodifier',
    'unbacom',
    'unified_maxslope',
    'unit_market',
    'unit_restrictions_noair',
    'unit_restrictions_noconverters',
    'unit_restrictions_noendgamelrpc',
    'unit_restrictions_noextractors',
    'unit_restrictions_nolrpc',
    'unit_restrictions_nonukes',
    'unit_restrictions_notacnukes',
    'unit_restrictions_notech2',
    'unit_restrictions_notech3',
    'usemapconfig',
    'usemexconfig',
}

float_columns = {
    'evocomleveluprate',
    'unbacomleveluprate',
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
}

logger = logging.getLogger()

if dev:
    logger.setLevel(logging.DEBUG)

nuttyb_hp_enum = pl.Enum(
    ['Epic', 'Epic+', 'Epic++', 'Epicer+', 'Epicer++', 'Epicest'],
)

nuttyb_hp_df = pl.DataFrame(
    [(hp, tweak) for hp, tweaks in nuttyb_hp_multiplier.items() for tweak in tweaks],
    schema=['nuttyb_hp', 'tweakdefs1'],
)


def has_player_handicap(allyTeams):
    for team in allyTeams:
        for player in team['Players']:
            if player['handicap'] > 0:
                return True
    return False


def has_barbarian(allyTeams):
    for team in allyTeams:
        for ai in team['AIs']:
            if ai['shortName'] == 'BarbarianAI':
                s()
                return True
    return False


def reorder_column(df, new_position, col_name):
    neworder = df.columns
    neworder.remove(col_name)
    neworder.insert(new_position, col_name)
    return df.select(neworder)


def reorder_tweaks(df):
    start_index = df.columns.index('tweakunits')
    for sub_index, tweak_column in enumerate(possible_tweak_columns[1:]):
        df = reorder_column(df, start_index + sub_index + 1, tweak_column)
    return df


def cast_frame(_df):
    columns_set = set(_df.columns)
    in_df_str_cols = list(columns_set & string_columns)
    in_df_num_cols = list(columns_set & numerical_columns)
    in_df_float_cols = list(columns_set & float_columns)

    accounted_for_columns = (
        string_columns
        | numerical_columns
        | float_columns
        | {
            'AllyTeams',
            'AllyTeamsList',
            'awards',
            'draw',
            'fetch_success',
            'id',
            'is_player_ai_mixed',
            'Map',
            'player_win',
            'players',
            'raptor_difficulty',
            'raptors_win',
            'raptors',
            'scav_difficulty',
            'scavengers_win',
            'scavengers',
            'startTime',
            'winners',
        }
    )

    other_cols = set(_df.columns) - accounted_for_columns
    if len(other_cols) > 0:
        logger.error(f'not casted cols: {other_cols}')
        raise Exception(f'not casted cols: {other_cols}')

    num_types = [
        pl.Boolean,
        pl.UInt8,
        pl.Int8,
        pl.UInt16,
        pl.Int16,
        pl.UInt32,
        pl.Int32,
        pl.UInt64,
        pl.Int64,
        pl.Float64,
    ]

    for col in in_df_num_cols:
        for _type in num_types:
            try:
                if _type == pl.Boolean and len(_df.filter(pl.col(col).ge(1))) > 0:
                    continue
            except pl.exceptions.ComputeError:
                continue

            try:
                _df = _df.cast({col: _type}, strict=True)
                break
            except Exception as e:
                logger.debug(e)

    difficulty_enum = pl.Enum(
        ['veryeasy', 'easy', 'normal', 'hard', 'veryhard', 'epic']
    )

    _df = reorder_tweaks(
        _df.join(
            nuttyb_hp_df,
            on='tweakdefs1',
            how='left',
        )
        .cast(
            {
                **{string_col: str for string_col in in_df_str_cols},
                **{float_col: pl.Float64 for float_col in in_df_float_cols},
                'nuttyb_hp': nuttyb_hp_enum,
                'scav_difficulty': difficulty_enum,
                'raptor_difficulty': difficulty_enum,
            },
            strict=True,
        )
        .with_columns(
            pl.col(in_df_str_cols).fill_null(''),
            pl.col('forceallunits').fill_null(0),
            pl.col('evocom').fill_null(0),
            pl.col('evocomleveluprate').fill_null(5),
            pl.col('evocomxpmultiplier').fill_null(1),
            *[
                pl.col(x).fill_null(0)
                for x in numerical_columns
                if 'unit_restrictions_' in x
            ],
            *[
                pl.col(x).fill_null(0)
                for x in [
                    'april1extra',
                    'april1',
                    'easteregghunt',
                    'easter_egg_hunt',
                ]
            ],
            has_player_handicap=pl.col('AllyTeams').map_elements(
                has_player_handicap, return_dtype=pl.Boolean
            ),
            barbarian=(
                pl.col('AllyTeamsList')
                if 'AllyTeamsList' in _df.columns
                else pl.col('AllyTeams')
            ).map_elements(has_barbarian, return_dtype=pl.Boolean),
        )
    )
    # _df[[s.name for s in _df if s.null_count() > 0] + ['startTime']].sort(
    #     'startTime'
    # ).glimpse()

    return _df
