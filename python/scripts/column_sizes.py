import os
from pprint import pprint
import polars as pl
import tqdm

from common.logger import get_logger
from pve_rating import user_ids_name_map
from common.common import LOCAL_DATA_DIR

if os.environ.get('ENV', 'prod') == 'dev':
    from bpdb import set_trace as s  # noqa: F401

logger = get_logger()

main_df = pl.read_parquet(os.path.join(LOCAL_DATA_DIR, 'replays_gamesettings.parquet'))
user_ids_names = user_ids_name_map(main_df)

column_sizes = tqdm(
    sorted(
        [
            (col, int(round(main_df[col].estimated_size('mb'), 1)))
            for col in main_df.columns
        ],
        key=lambda x: x[1],
        reverse=True,
    )
)

pprint(column_sizes)
s()
