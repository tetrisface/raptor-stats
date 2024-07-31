import os
from bpdb import set_trace as s
import polars as pl
import polars.selectors as cs

from common.logger import get_logger

logger = get_logger()

main_df = pl.read_parquet('replays_gamesettings.parquet')


s()
count = 0
for root, dirs, files in os.walk('backups'):
    for file in files:
        if 'replays' not in file:
            continue

        print(f'Found file: {os.path.join(root, file)}')
        count += 1

        x = pl.read_parquet(os.path.join(root, file))

        if 'awards' in x.columns:
            main_df = (
                main_df.join(
                    x['id', 'awards'],
                    how='left',
                    on='id',
                    validate='1:1',
                    coalesce=True,
                )
                .with_columns(
                    pl.when(
                        pl.col('awards')
                        .struct.field('fightingUnitsDestroyed')
                        .list[0]
                        .struct.field('value')
                        .is_null()
                        & pl.col('awards_right')
                        .struct.field('fightingUnitsDestroyed')
                        .list[0]
                        .struct.field('value')
                        .is_not_null()
                    )
                    .then(pl.col('awards_right'))
                    .otherwise(pl.col('awards'))
                    .alias('awards')
                )
                .drop(cs.ends_with('_right'))
            )

        if 'Map' in x.columns:  # and len(x.filter(pl.col('Map').is_null())):
            main_df = (
                main_df.join(
                    x['id', 'Map'],
                    how='left',
                    on='id',
                    validate='1:1',
                    coalesce=True,
                )
                .with_columns(
                    pl.when(
                        pl.col('Map').struct.field('scriptName').is_null()
                        & pl.col('Map_right').struct.field('scriptName').is_not_null()
                    )
                    .then(pl.col('Map_right'))
                    .otherwise(pl.col('Map'))
                    .alias('Map'),
                )
                .drop(cs.ends_with('_right'))
            )

        if 'Map Name' in x.columns:
            main_df = (
                main_df.join(
                    x['id', 'Map Name'],
                    how='left',
                    on='id',
                    validate='1:1',
                    coalesce=True,
                )
                .with_columns(
                    pl.when(
                        pl.col('Map Name').is_null()
                        & pl.col('Map Name_right').is_not_null()
                    )
                    .then(pl.col('Map Name_right'))
                    .otherwise(pl.col('Map Name'))
                    .alias('Map Name'),
                )
                .drop(cs.ends_with('_right'))
            )

logger.info(
    f'Null awards {before_null_awards} -> {len(main_df.filter(pl.col('awards').struct.field('fightingUnitsDestroyed').list[0].struct.field('value').is_null()))}'
)
logger.info(
    f'Null Map {before_null_map} -> {len(main_df.filter(pl.col('Map').struct.field('scriptName').is_null()))}'
)
logger.info(
    f'Null Map Name {before_null_map_name} -> {len(main_df.filter(pl.col("Map Name").is_null()))}'
)

main_df = main_df.with_columns(
    pl.when(
        pl.col('awards')
        .struct.field('fightingUnitsDestroyed')
        .list[0]
        .struct.field('value')
        .is_null()
        | pl.col('Map').struct.field('scriptName').is_null()
        | pl.col('Map Name').is_null()
    )
    .then(None)
    .otherwise(True)
    .alias('fetch_success')
)

main_df.write_parquet('replays_gamesettings.parquet')
