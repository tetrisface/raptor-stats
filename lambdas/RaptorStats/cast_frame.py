import logging
import os

import polars as pl

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

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
    'evocomleveluprate',
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
string_columns = {
    'assistdronesenabled',
    'commanderbuildersenabled',
    'deathmode',
    'draft_mode',
    'experimentalshields',
    'experimentalstandardgravity',
    'lootboxes_density',
    'lootboxes',
    'map_tidal',
    'raptor_difficulty',
    'raptor_raptorstart',
    'ruins_density',
    'ruins',
    'scav_difficulty',
    'scav_scavstart',
    'scoremode',
    'teamcolors_anonymous_mode',
    'teamcolors_icon_dev_mode',
    'transportenemy',
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
}

logger = logging.getLogger()

if dev:
    logger.setLevel(logging.DEBUG)


def cast_frame(_df):
    _in_str_cols = list(set(_df.columns) & string_columns)
    _in_num_cols = list(set(_df.columns) & numerical_columns)

    other_cols = list(
        set(_df.columns)
        - set(_in_str_cols)
        - set(_in_num_cols)
        - {
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
            'raptor_win',
            'raptors_win',
            'raptors',
            'scavengers_win',
            'scavengers',
            'startTime',
            'winners',
        }
    )
    if len(other_cols) > 0:
        logger.warning(f'not casted cols: {other_cols}')

    _df = _df.cast({col: str for col in _in_str_cols}).with_columns(
        pl.col(_in_str_cols).fill_null('')
    )

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
        # pl.Float32,
        pl.Float64,
    ]

    for col in _in_num_cols:
        for _type in num_types:
            try:
                _df = _df.cast({col: _type}, strict=True)
                break
            except Exception as e:
                logger.debug(e)

    return _df
