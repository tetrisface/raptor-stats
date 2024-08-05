import sys
import polars as pl
from bpdb import set_trace as s

from common.common import s3_download_df

user_id_names = s3_download_df('', 'replays_gamesettings.parquet')

# read cmd arg

user_id_names = (
    user_id_names.sort('startTime', descending=False)
    .select(
        pl.col('AllyTeams')
        .list.eval(
            pl.element()
            .struct['Players']
            .list.eval(
                pl.struct(
                    pl.element().struct['userId'].cast(pl.UInt32),
                    pl.element().struct['name'],
                )
            )
            .flatten()
            .drop_nulls()
        )
        .explode()
    )
    .unnest('AllyTeams')
    .drop_nulls()
    .unique()
)

if len(sys.argv) > 1:
    user_id = user_id_names.filter(pl.col('name') == 'Deocy')['userId'][0]

    print(user_id_names.filter(pl.col('userId') == user_id))

s()
