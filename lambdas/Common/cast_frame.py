import datetime
import os
import random

import numpy as np
import polars as pl
from Common.gamesettings import nuttyb_hp_multiplier, possible_tweak_columns
from Common.logger import get_logger

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
    'evocomlevelupmethod',
    'experimentalshields',
    'experimentalstandardgravity',
    'lootboxes_density',
    'lootboxes',
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

int_columns = {
    'ai_incomemultiplier',
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


def drop_null_empty_cols(_df):
    before_drop_cols = set(_df.columns)
    _df = _df[
        [
            s.name
            for s in _df
            if not (
                (s.null_count() == _df.height) | (s.dtype == str and (s == '').all())
            )
        ]
    ]
    logger.info(f'Dropped columns: {before_drop_cols - set(_df.columns)}')
    return _df


def awards(row):
    ids_names = {}
    for team in row['AllyTeams']:
        for player in team['Players']:
            if 'Players' in team:
                ids_names[player['teamId']] = player['name']

    damage_award = None
    eco_award = None

    try:
        damage_award = ids_names[row['awards']['fightingUnitsDestroyed'][0]['teamId']]
    except KeyError:
        pass

    try:
        eco_award = ids_names[row['awards']['mostResourcesProduced']['teamId']]
    except KeyError:
        pass

    return {
        'damage_award': damage_award,
        'eco_award': eco_award,
    }


def add_computed_cols(_df):
    logger.info('Adding computed columns')
    _df = _df.filter(
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
    ).with_columns(
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
        barbarian=pl.col('AllyTeams')
        .list.eval(
            pl.element()
            .struct['AIs']
            .list.eval(pl.element().struct['shortName'])
            .flatten()
        )
        .list.contains('BARb'),
        is_player_ai_mixed=(pl.col('AllyTeams').list.len() > 2)
        | pl.col('AllyTeams')
        .list.eval(
            (pl.element().struct['Players'].list.len() > 0)
            & (pl.element().struct['AIs'].list.len() > 0)
        )
        .list.any(),
        draw=(pl.col('durationMs') == 0)
        | pl.col('AllyTeams').list.eval(~pl.element().struct['winningTeam']).list.all(),
        winners=pl.col('AllyTeams').list.eval(
            pl.when(pl.element().struct['winningTeam'])
            .then(pl.element().struct['Players'].list.eval(pl.element().struct['name']))
            .flatten()
            .drop_nulls()
        ),
        players=pl.col('AllyTeams').list.eval(
            pl.element()
            .struct['Players']
            .list.eval(pl.element().struct['name'])
            .flatten()
            .drop_nulls()
        ),
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
    )

    if 'AllyTeamsList' in _df.columns:
        _df = _df.join(
            _df.filter(
                pl.col('AllyTeams').is_not_null() & pl.col('awards').is_not_null()
            ).with_columns(
                pl.struct('AllyTeams', 'awards')
                .map_elements(awards, return_dtype=pl.Struct)
                .alias('damage_eco_award')
            )['id', 'damage_eco_award'],
            on='id',
            how='left',
        )

        _df = _df.with_columns(
            pl.col('Map').struct.field('scriptName').alias('Map Name'),
            barbarian_handicap=pl.when('barbarian')
            .then(
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
            )
            .otherwise(None),
            barbarian_per_player=pl.col('AllyTeams')
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
            .flatten(),
            has_player_handicap=pl.col('AllyTeams')
            .list.eval(
                pl.element()
                .struct['Players']
                .list.eval(pl.element().struct['handicap'] > 0)
                .flatten()
                .any()
            )
            .flatten(),
        )
    return _df


def cast_frame(_df):
    columns_set = set(_df.columns)
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
            'barbarian',
            'barbarian_handicap',
            'barbarian_per_player',
            'barbarian_win',
            'draw',
            'fetch_success',
            'id',
            'is_player_ai_mixed',
            'Map',
            'Map Name',
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

    other_cols = set(_df.columns) - accounted_for_columns
    if len(other_cols) > 0:
        logger.error(f'New not casted cols: {other_cols}')
        if random.randint(0, 1) == 0:
            raise ValueError(f'New not casted cols: {other_cols}')

    for col in in_df_str_cols:
        _df = _df.with_columns(pl.col(col).cast(pl.Utf8))

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
        pl.Decimal,
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
            except Exception:
                pass

    if _df[['startTime']].dtypes[0] != pl.Datetime:
        _df = _df.with_columns(
            pl.col('startTime').str.to_datetime(
                '%+', time_unit='ns', time_zone='UTC', strict=True, exact=True
            )
        )
    for col in in_df_float_cols:
        if '_spawntimemult' in col:
            _df = _df.cast(
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

    _df = reorder_tweaks(
        _df.join(
            nuttyb_hp_df,
            on='tweakdefs1',
            how='left',
        ).cast(
            {
                **{string_col: str for string_col in in_df_str_cols},
                'nuttyb_hp': nuttyb_hp_enum,
                'scav_difficulty': difficulty_enum,
                'raptor_difficulty': difficulty_enum,
                'assistdronesenabled': enabled_disabled_enum,
                'commanderbuildersenabled': enabled_disabled_enum,
            },
            strict=True,
        )
    )

    for col in in_df_decimal_cols:
        for decimal_type in [pl.Decimal(None, 1), pl.Float64]:
            try:
                _df = _df.cast(
                    {col: decimal_type},
                    strict=True,
                )
            except Exception:
                logger.error(
                    f'Could not cast decimal column "{col}" to "{decimal_type}"'
                )
    _df = _df.with_columns(
        pl.col(in_df_str_cols).fill_null(''),
        pl.col('evocomleveluprate').fill_null(5).alias('evocomleveluprate')
        if 'evocomleveluprate' in _df.columns
        else None,
        *[
            pl.col(x).fill_null(0)
            for x in int_columns
            if 'unit_restrictions_' in x and x in _df.columns
        ],
        *[
            pl.col(x).fill_null(0)
            for x in [
                'april1',
                'april1extra',
                'easter_egg_hunt',
                'easteregghunt',
                'evocom',
                'forceallunits',
            ]
            if x in _df.columns
        ],
        *[
            pl.col(x).fill_null(1)
            for x in [
                'evocomxpmultiplier',
                'multiplier_maxdamage',
                'multiplier_energycost',
                'multiplier_metalcost',
                'multiplier_buildtimecost',
                'scav_graceperiodmult',
            ]
            if x in _df.columns
        ],
    )
    # _df[[s.name for s in _df if s.null_count() > 0] + ['startTime']].sort(
    #     'startTime'
    # ).glimpse()

    return _df
