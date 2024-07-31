from common.common import (
    FILE_SERVE_BUCKET,
    READ_DATA_BUCKET,
    WRITE_DATA_BUCKET,
    replay_details_file_name,
    s3_download_df,
    s3_upload_df,
)
from bpdb import set_trace as s
import polars as pl
import polars.selectors as cs

from common.cast_frame import cast_frame
from common.logger import get_logger

logger = get_logger()

df = cast_frame(s3_download_df(READ_DATA_BUCKET, replay_details_file_name))

df = df.with_columns(
    pl.when(
        pl.col('awards')
        .struct.field('fightingUnitsDestroyed')
        .list[0]
        .struct.field('value')
        .is_null()
        | pl.col('Map Name').is_null()
    )
    .then(None)
    .otherwise(pl.col('fetch_success'))
    .alias('fetch_success'),
    # pl.when(pl.col('evocomlevelupmethod').is_null())
    # .then(None)
    # .otherwise(pl.col('fetch_success'))
    # .alias('fetch_success'),
    # pl.col('comrespawn').fill_null('disabled'),
    # pl.col('no_comtrans').cast(pl.Boolean).fill_null(False),
    # pl.when(
    #     pl.col('evocomlevelupmethod').is_null() | pl.col('evocomlevelupmethod').eq('')
    # ).then(
    #     pl.when(pl.col('startTime').dt.date().lt(datetime.date(2024, 6, 28)))
    #     .then(pl.lit('dynamic').alias('evocomlevelupmethod'))
    #     .otherwise(pl.lit(None).alias('fetch_success'))
    # )
    # pl.when(
    #     pl.col('evocomlevelupmethod').eq('timed')
    #     & pl.col('startTime')
    #     .dt.date()
    #     .is_between(
    #         datetime.date(2024, 6, 28), datetime.date(2024, 7, 7), closed='both'
    #     )
    # )
    # .then(False)
    # .otherwise(True)
    # .alias('fetch_success'),
    # pl.when(pl.col('tweakunits8').is_null())
    # .then(False)
    # .otherwise(pl.col('fetch_success'))
    # .alias('fetch_success'),
    # pl.when(pl.col('fetch_success').eq(False))
    # .then(None)
    # .otherwise(pl.col('fetch_success'))
    # .alias('fetch_success'),
).drop(cs.ends_with('_right'))
s()
s3_upload_df(df, WRITE_DATA_BUCKET, replay_details_file_name)
s3_upload_df(df, FILE_SERVE_BUCKET, replay_details_file_name)
