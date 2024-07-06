from Common.common import get_df, replay_details_file_name
from bpdb import set_trace as s
import polars as pl

from RaptorStats.raptor_stats import store_df

df = get_df(replay_details_file_name)

df = df.with_columns(
    pl.col('comrespawn').fill_null('disabled'),
    pl.col('no_comtrans').cast(pl.Boolean).fill_null(False),
    pl.col('evocomlevelupmethod').fill_null('timed'),
)
store_df(df, replay_details_file_name)
