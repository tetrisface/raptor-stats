import os
import re
import time

import orjson
import polars as pl
import polars.selectors as cs
from bpdb import set_trace as s
from common.cast_frame import map_replace_regex_string, reorder_column
from common.common import LOCAL_DATA_DIR
from common.logger import get_logger
from util.create_ann_index import get_hnsw_index

bar_dir = f'/mnt/c/Users/{os.environ["WINDOWS_USERNAME"]}/AppData/Local/Programs/Beyond-All-Reason/data/'
modoptions_file = os.path.join(bar_dir, 'modoptions.json')
players_file = os.path.join(bar_dir, 'users.json')
bot_file = os.path.join(bar_dir, 'bot.txt')
map_file = os.path.join(bar_dir, 'map.txt')

logger = get_logger()

ratings = {
    'Barbarian': pl.read_parquet(
        os.path.join(LOCAL_DATA_DIR, 'PveRating.Barbarian_gamesettings.parquet')
    ),
    'Raptors': pl.read_parquet(
        os.path.join(LOCAL_DATA_DIR, 'PveRating.Raptors_gamesettings.parquet')
    ),
    'Scavengers': pl.read_parquet(
        os.path.join(LOCAL_DATA_DIR, 'PveRating.Scavengers_gamesettings.parquet')
    ),
}

ratings = {
    k: reorder_column(
        v.with_row_index().rename({'index': '#'}).drop(cs.contains('Rank')),
        2,
        'PVE Rating',
    )
    for k, v in ratings.items()
}


def watch_file(file_path, callback, interval=1):
    # Get the initial last modified time
    last_modified_time = os.path.getmtime(file_path)

    try:
        while True:
            time.sleep(interval)
            try:
                # Check the current last modified time
                current_modified_time = os.path.getmtime(file_path)

                if current_modified_time != last_modified_time:
                    last_modified_time = current_modified_time
                    callback()
            except FileNotFoundError:
                # Handle the case where the file might not exist temporarily
                print(f'File {file_path} not found. Retrying...')
    except KeyboardInterrupt:
        print('Stopped watching the file.')


# with open('hnsw_index.pickle', 'rb') as file:
#     ann_index = pickle.loads(file.read())
# with open('hnsw_preprocessor.pickle', 'rb') as file:
#     preprocessor = pickle.loads(file.read())

prefixed_indices_preprocessors = {
    _prefix: get_hnsw_index(_prefix)
    for _prefix in ['Barbarian', 'Raptors', 'Scavengers']
}

for _prefix, index_preprocessor in prefixed_indices_preprocessors.items():
    index_preprocessor.ann_index.set_ef(60000)

logger.info('Loaded indices and preprocessors')


pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_dtype_separator(True)
pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_cols(100)
pl.Config.set_tbl_rows(100)


def on_modoptions():
    previous_modoptions = {}
    try:
        with open(modoptions_file) as f:
            modoptions = orjson.loads(f.read())
            if modoptions == previous_modoptions:
                return
            previous_modoptions = modoptions
        with open(players_file) as f:
            players = [k for k in orjson.loads(f.read()) if not k.startswith('Host[')]
        with open(bot_file) as f:
            ai = f.read().lower()
        with open(map_file) as f:
            map = re.sub(map_replace_regex_string, '', f.read())
    except TypeError as e:
        if 'iterable' in str(e):
            logger.error(e)
            logger.info('Started with debug launcher?')
        return
    except Exception as e:
        logger.error(e)
        return

    if re.search(r'\d', ai):
        prefix = 'Barbarian'
    elif 'scavengers' in ai:
        prefix = 'Scavengers'
    else:
        prefix = 'Raptors'

    df = pl.DataFrame(modoptions).with_columns(
        pl.lit(map).alias('Map'), pl.lit(1).alias('scav_graceperiodmult')
    )

    try:
        processed = prefixed_indices_preprocessors[prefix].preprocessor.transform(df)
        if hasattr(processed, 'toarray'):
            processed = processed.toarray()

    except ValueError as e:
        logger.error(e)
        return

    labels, distances = prefixed_indices_preprocessors[prefix].ann_index.knn_query(
        processed, k=8
    )

    result_df = (
        pl.DataFrame(
            zip(labels[0], distances[0]),
            schema=['Difficulty', 'distance'],
        )
        .with_columns((pl.col('Difficulty') / 1e6))
        .with_row_index()
    )

    players_info_df = (
        ratings[prefix].filter(pl.col('Player').is_in(players)).with_row_index()
    )
    result_df = reorder_column(
        result_df.join(players_info_df, on='index', how='full')
        .drop('index', 'index_right')
        .with_columns(
            (~cs.matches('^Difficulty$') & ~cs.matches('^distance$') & cs.numeric())
            .round(2)
            .cast(pl.String)
        )
        .fill_null(''),
        3,
        'Player',
    )

    print(
        result_df.with_columns(
            pl.col('Difficulty').round(1), pl.col('distance').round(1)
        )
        .cast(pl.String)
        .with_columns(
            pl.all().str.replace_all(r'\.0+$', '').str.replace_all(r'^0$', '')
        )
        .fill_null('')
    )

    print(
        result_df['Difficulty', 'distance']
        .hstack(
            pl.DataFrame(
                [players_info_df['PVE Rating'].mean()] * len(result_df),
                schema=['PVE Rating'],
            )
        )
        .mean()
        .with_columns(pl.col('Difficulty').round(1), pl.col('distance').round(1))
        .cast(pl.String)
        .with_columns(pl.all().str.replace_all(r'\.0+$', ''))
    )


if __name__ == '__main__':
    logger.info(f'Starting watch {modoptions_file}')
    on_modoptions()
    watch_file(modoptions_file, on_modoptions)
