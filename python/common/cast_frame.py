import datetime
import os
import random

import numpy as np
import polars as pl
from common.gamesettings import (
    nuttyb_hp_multiplier,
    possible_tweak_columns,
)
from common.logger import get_logger

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401


string_columns = {
    'assistdronesenabled',
    'commanderbuildersenabled',
    'comrespawn',
    'deathmode',
    'draft_mode',
    'experimentalshields',
    'experimentalstandardgravity',
    'lootboxes_density',
    'lootboxes',
    'map_tidal',
    'mapmetadata_startpos',
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

int_columns = {
    'accuratelasers',  # todo delete
    'ai_incomemultiplier',  # todo delete
    'air_rework',
    'allowpausegameplay',
    'allowuserwidgets',
    'animationcleanup',
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
    'evocomlevelcap',
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
    'no_comtrans',
    'norush',
    'norushtimer',
    'numberofcontrolpoints',
    'proposed_unit_reworks',
    'ranked_game',
    'raptor_endless',
    'raptor_spawncountmult',
    'releasecandidates',
    'ruins_civilian_disable',
    'ruins_only_t1',
    'scav_endless',
    'scav_spawncountmult',
    'scoremode_chess_adduptime',
    'scoremode_chess_spawnsperphase',
    'scoremode_chess_unbalanced',
    'scoremode_chess',
    'shareddynamicalliancevictory',
    'skyshift',
    'slow_comtrans',
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

decimal_columns = {
    'evocomleveluprate',
    'evocomxpmultiplier',
    'experimentalxpgain',
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
    'raptor_firstwavesboost',
    'raptor_graceperiodmult',
    'raptor_queentimemult',
    'scav_bosstimemult',
    'scav_graceperiodmult',
    'unbacomleveluprate',
}

float_columns = {
    'raptor_spawntimemult',
    'scav_spawntimemult',
}

nuttyb_hp_enum = pl.Enum(
    ['Epic', 'Epic+', 'Epic++', 'Epicer+', 'Epicer++', 'Epicest'],
)

nuttyb_hp_df = pl.DataFrame(
    [(hp, tweak) for hp, tweaks in nuttyb_hp_multiplier.items() for tweak in tweaks],
    schema=['nuttyb_hp', 'tweakdefs1'],
    orient='row',
)

map_replace_regex_string = r'(?i)[_\s]+[v\d\.]+\w*$'


def reorder_column(df, new_position, col_name):
    if col_name not in df.columns:
        return df
    neworder = df.columns
    neworder.remove(col_name)
    neworder.insert(new_position, col_name)
    return df.select(neworder)


def reorder_tweaks(df):
    if 'tweakunits' not in df.columns:
        return df
    start_index = df.columns.index('tweakunits')
    for sub_index, tweak_column in enumerate(possible_tweak_columns[1:]):
        df = reorder_column(df, start_index + sub_index + 1, tweak_column)
    return df


def drop_null_empty_cols(df):
    before_drop_cols = set(df.columns)
    df = df[
        [
            s.name
            for s in df
            if not (
                (s.null_count() == df.height) | (s.dtype == str and (s == '').all())
            )
        ]
    ]
    logger.info(f'Dropped columns: {before_drop_cols - set(df.columns)}')
    return df


def awards(row):
    damage_award = None
    damage_award_value = None
    eco_award = None

    if not row.get('AllyTeams'):
        return {
            'damage_award': damage_award,
            'damage_award_value': damage_award_value,
            'eco_award': eco_award,
        }

    ids_names = {}
    if row.get('AllyTeams') is not None:
        for team in row.get('AllyTeams', []):
            try:
                for player in team.get('Players', []):
                    if 'teamId' in player and 'userId' in player:
                        ids_names[player['teamId']] = player['userId']
            except Exception:
                pass

    try:
        damage_award = np.uint32(
            ids_names[row['awards']['fightingUnitsDestroyed'][0]['teamId']]
        )
        damage_award_value = np.uint32(
            row['awards']['fightingUnitsDestroyed'][0]['value']
        )
    except Exception:
        pass

    try:
        eco_award = np.uint32(
            ids_names[row['awards']['mostResourcesProduced']['teamId']]
        )
    except Exception:
        pass

    logger.debug(f'awards returned {damage_award} {eco_award}')
    return {
        'damage_award': damage_award,
        'damage_award_value': damage_award_value,
        'eco_award': eco_award,
    }


def add_computed_cols(df):
    logger.info('Filtering games by AllyTeamsList info')
    df = df.filter(
        pl.col('AllyTeams')
        .list.eval(
            pl.element()
            .struct['AIs']
            .list.eval(
                pl.element()
                .struct['shortName']
                .is_in(['BARb', 'RaptorsAI', 'ScavengersAI'])
            )
            .flatten()
            .drop_nulls()
        )
        .list.all()
        & pl.col('durationMs').gt(0)
        & pl.col('AllyTeams').list.eval(pl.element().struct['winningTeam']).list.any()
        & pl.col('AllyTeams').list.len().lt(3)
        & pl.col('AllyTeams')
        .list.eval(
            pl.element().struct['Players'].list.len().eq(0)
            | pl.element().struct['AIs'].list.len().eq(0)
        )
        .list.all()
    )

    if 'AllyTeamsList' in df.columns:
        df = df.filter(
            ~pl.col('AllyTeams')
            .list.eval(
                pl.element()
                .struct['Players']
                .list.eval(pl.element().struct['handicap'] > 0)
                .flatten()
                .any()
            )
            .flatten()
        )

    logger.info('Adding computed columns')
    df = df.with_columns(
        barbarian=pl.col('AllyTeams')
        .list.eval(
            pl.element()
            .struct['AIs']
            .list.eval(pl.element().struct['shortName'])
            .flatten()
        )
        .list.contains('BARb'),
        raptors=pl.col('AllyTeams')
        .list.eval(
            pl.element()
            .struct['AIs']
            .list.eval(pl.element().struct['shortName'])
            .flatten()
        )
        .list.contains('RaptorsAI'),
        scavengers=pl.col('AllyTeams')
        .list.eval(
            pl.element()
            .struct['AIs']
            .list.eval(pl.element().struct['shortName'])
            .flatten()
        )
        .list.contains('ScavengersAI'),
        barbarian_win=pl.col('AllyTeams')
        .list.eval(
            pl.when(pl.element().struct['winningTeam'])
            .then(
                pl.element()
                .struct['AIs']
                .list.eval(pl.element().struct['shortName'] == 'BARb')
            )
            .flatten()
            .drop_nulls()
            .any()
        )
        .flatten(),
        raptors_win=pl.col('AllyTeams')
        .list.eval(
            pl.when(pl.element().struct['winningTeam'])
            .then(
                pl.element()
                .struct['AIs']
                .list.eval(pl.element().struct['shortName'] == 'RaptorsAI')
            )
            .flatten()
            .drop_nulls()
            .any()
        )
        .flatten(),
        scavengers_win=pl.col('AllyTeams')
        .list.eval(
            pl.when(pl.element().struct['winningTeam'])
            .then(
                pl.element()
                .struct['AIs']
                .list.eval(pl.element().struct['shortName'] == 'ScavengersAI')
            )
            .flatten()
            .drop_nulls()
            .any()
        )
        .flatten(),
    )

    if 'AllyTeamsList' in df.columns:
        if 'nuttyb_hp' not in df.columns:
            df = df.join(
                nuttyb_hp_df,
                on='tweakdefs1',
                how='left',
            )
        df = df.join(
            df.filter(
                pl.col('AllyTeams').is_not_null() & pl.col('awards').is_not_null()
            ).with_columns(
                pl.struct('AllyTeams', 'awards')
                .drop_nulls()
                .map_elements(awards, return_dtype=pl.Struct, skip_nulls=True)
                .alias('damage_eco_award')
            )['id', 'damage_eco_award'],
            on='id',
            how='left',
        )

        df = df.with_columns(
            pl.when(pl.col('Map Name').is_null())
            .then(
                pl.col('Map')
                .struct.field('scriptName')
                .str.replace(
                    map_replace_regex_string,
                    '',
                )
            )
            .otherwise(pl.col('Map Name'))
            .alias('Map Name')
            if {'Map Name', 'Map'}.issubset(set(df.columns))
            else None,
            pl.when('barbarian')
            .then(
                (
                    pl.col('AllyTeams')
                    .list.eval(
                        pl.element()
                        .struct['AIs']
                        .list.eval(
                            pl.when(pl.element().struct['shortName'] == 'BARb').then(
                                pl.element().struct['handicap']
                            )
                        )
                        .flatten()
                        .drop_nulls()
                        .mean()
                    )
                    .flatten()
                ).round()
            )
            .otherwise(None)
            .cast(pl.UInt8)
            .alias('Barbarian Handicap'),
            (
                pl.col('AllyTeams')
                .list.eval(
                    pl.element()
                    .struct['AIs']
                    .list.eval(
                        pl.when(pl.element().struct['shortName'] == 'BARb')
                        .then(1)
                        .otherwise(0)
                    )
                    .flatten()
                    .sum()
                )
                .flatten()
                / pl.col('AllyTeams')
                .list.eval(pl.element().struct['Players'].list.len().sum())
                .flatten()
            )
            .round(1)
            .cast(pl.Float32)
            .alias('Barbarian Per Player'),
            winners=pl.col('AllyTeams').list.eval(
                pl.when(pl.element().struct['winningTeam'])
                .then(
                    pl.element()
                    .struct['Players']
                    .list.eval(pl.element().struct['userId'].cast(pl.UInt32))
                )
                .flatten()
                .drop_nulls()
            ),
            players=pl.col('AllyTeams').list.eval(
                pl.element()
                .struct['Players']
                .list.eval(pl.element().struct['userId'].cast(pl.UInt32))
                .flatten()
                .drop_nulls()
            ),
        )
    return df


def cast_frame(df):
    columns_set = set(df.columns)
    in_df_str_cols = list(columns_set & string_columns)
    in_df_num_cols = list(columns_set & int_columns)
    in_df_decimal_cols = list(columns_set & decimal_columns)
    in_df_float_cols = list(columns_set & float_columns)

    accounted_for_columns = (
        string_columns
        | int_columns
        | decimal_columns
        | float_columns
        | {
            'AllyTeams',
            'AllyTeamsList',
            'awards',
            'Barbarian Handicap',
            'Barbarian Per Player',
            'barbarian_win',
            'barbarian',
            'draw',  # TODO delete
            'evocomlevelupmethod',
            'fetch_success',
            'id',
            'is_player_ai_mixed',
            'Map Name',
            'Map',
            'nuttyb_hp',
            'player_win',  # TODO delete
            'players',
            'raptor_difficulty',
            'raptors_win',
            'raptors',
            'scav_difficulty',
            'scavengers_win',
            'scavengers',
            'startTime',
            'supported_ais',
            'winners',
        }
    )

    other_cols = set(df.columns) - accounted_for_columns
    if len(other_cols) > 0:
        logger.error(f'New not casted cols: {other_cols}')
        if random.randint(0, 1) == 0:
            raise ValueError(f'New not casted cols: {other_cols}')

    for col in in_df_str_cols:
        df = df.with_columns(pl.col(col).cast(pl.Utf8))

    num_types = [
        pl.UInt8,
        pl.Int8,
        pl.UInt16,
        pl.Int16,
        pl.UInt32,
        pl.Int32,
        pl.UInt64,
        pl.Int64,
        pl.Decimal,
        pl.Float64,
    ]

    for col in in_df_num_cols:
        for _type in num_types:
            try:
                if _type == pl.Boolean and len(df.filter(pl.col(col).ge(1))) > 0:
                    continue
            except pl.exceptions.ComputeError:
                continue

            try:
                df = df.cast({col: _type}, strict=True)
                break
            except Exception:
                pass

    if df[['startTime']].dtypes[0] != pl.Datetime:
        df = df.with_columns(
            pl.col('startTime').str.to_datetime(
                '%+', time_unit='ns', time_zone='UTC', strict=True, exact=True
            )
        )
    for col in in_df_float_cols:
        if '_spawntimemult' in col:
            df = df.cast(
                {col: pl.Float64}, strict=True
            ).with_columns(
                pl.when(
                    pl.col('startTime').dt.date().gt(datetime.date(2024, 6, 5))
                )  # multiplier was inverted https://github.com/beyond-all-reason/Beyond-All-Reason/pull/3107/files
                .then(pl.col(col).fill_null(1.0))
                .otherwise(1 / pl.col(col).fill_null(1.0))
                .replace(np.inf, 1.0)
            )

    difficulty_enum = pl.Enum(
        ['veryeasy', 'easy', 'normal', 'hard', 'veryhard', 'epic']
    )

    enabled_disabled_enum = pl.Enum(['disabled', 'enabled'])

    df = reorder_tweaks(
        df.cast(
            {
                col: type
                for col, type in {
                    **{string_col: str for string_col in in_df_str_cols},
                    'nuttyb_hp': nuttyb_hp_enum,
                    'scav_difficulty': difficulty_enum,
                    'raptor_difficulty': difficulty_enum,
                    'assistdronesenabled': enabled_disabled_enum,
                    'commanderbuildersenabled': enabled_disabled_enum,
                }.items()
                if col in df.columns
            },
            strict=True,
        )
    )

    for col in in_df_decimal_cols:
        for decimal_type in [pl.Decimal(None, 1), pl.Float64]:
            try:
                df = df.cast(
                    {col: decimal_type},
                    strict=True,
                )
            except Exception:
                logger.error(
                    f'Could not cast decimal column "{col}" to "{decimal_type}"'
                )

    df = df.with_columns(
        *[
            pl.col(x).fill_null(fill).alias(x)
            for (x, fill) in [
                ('evocomleveluprate', 5),
                ('evocomlevelcap', 10),
                ('evocomlevelupmethod', 'dynamic'),
            ]
            if x in df.columns
        ],
        *[pl.col(x).fill_null('').alias(x) for x in in_df_str_cols],
        *[
            pl.col(x).fill_null(0).alias(x)
            for x in int_columns
            if 'unit_restrictions_' in x and x in df.columns
        ],
        *[
            pl.col(x).fill_null(0).alias(x)
            for x in [
                'april1',
                'april1extra',
                'easter_egg_hunt',
                'easteregghunt',
                'evocom',
                'forceallunits',
            ]
            if x in df.columns
        ],
        *[
            pl.col(x).fill_null(1).alias(x)
            for x in [
                'evocomxpmultiplier',
                'multiplier_maxdamage',
                'multiplier_energycost',
                'multiplier_metalcost',
                'multiplier_buildtimecost',
                'scav_graceperiodmult',
            ]
            if x in df.columns
        ],
    )

    return df
