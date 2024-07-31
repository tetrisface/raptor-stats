from pprint import pprint
from bpdb import set_trace as s
import polars as pl

from common.logger import get_logger
from pve_rating import user_ids_name_map

logger = get_logger()

main_df = pl.read_parquet('replays_gamesettings.parquet')
user_ids_names = user_ids_name_map(main_df)

main_df = main_df.with_columns(
    pl.col('players')
    .list.eval(pl.element().replace(user_ids_names))
    .alias('players_str')
)

col_sizes = sorted(
    [
        (col, int(round(main_df[col].estimated_size('mb'), 1)))
        for col in main_df.columns
    ],
    key=lambda x: x[1],
    reverse=True,
)

pprint(col_sizes)
