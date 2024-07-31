import os
import pickle
import pprint
from types import SimpleNamespace
from typing import Counter
import hnswlib
import polars as pl
import polars.selectors as cs

from bpdb import set_trace as s

from common.logger import get_logger
from common.common import LOCAL_DATA_DIR, grouped_gamesettings_preprocessor

logger = get_logger()


def get_hnsw_index(prefix):
    index_path = os.path.join(LOCAL_DATA_DIR, f'{prefix}.hnsw_index.pickle')
    preprocessor_path = os.path.join(
        LOCAL_DATA_DIR, f'{prefix}.hnsw_preprocessor.pickle'
    )
    # if os.path.exists(index_path) and os.path.exists(preprocessor_path):
    #     logger.info(f'Loading {prefix} index')
    #     with open(index_path, 'rb') as file:
    #         ann_index = pickle.loads(file.read())
    #     with open(preprocessor_path, 'rb') as file:
    #         preprocessor = pickle.loads(file.read())
    #     return SimpleNamespace(ann_index=ann_index, preprocessor=preprocessor)

    logger.info(
        f'Creating {prefix} hierarchical navigable small world approximate nearest neighbor index'
    )
    df = pl.read_parquet(
        os.path.join(LOCAL_DATA_DIR, f'{prefix}.all.grouped_gamesettings.parquet')
    ).filter(pl.col('#Players') >= (200 if prefix == 'Raptors' else 20))

    diffs = (
        df.select('Difficulty')
        .with_row_index()
        .with_columns(
            (pl.col('Difficulty') * 1e8).round(0).cast(pl.UInt32, strict=True)
            + pl.col('index')
        )['Difficulty']
    )

    df = df.drop(
        'Difficulty',
        '#Winners',
        'Winners',
        'Players',
        '#Players',
        'Merged Win Replays',
        'Merged Loss Replays',
        'Win Replays',
        'Loss Replays',
        'Copy Paste',
    )

    categorical_cols = df.select(cs.string()).columns
    numerical_cols = df.select(cs.numeric()).columns

    preprocessor = grouped_gamesettings_preprocessor(numerical_cols, categorical_cols)

    # Apply transformations
    df_preprocessed = preprocessor.fit_transform(df)

    if hasattr(df_preprocessed, 'toarray'):
        df_preprocessed = df_preprocessed.toarray()

    categorical_feature_names = (
        preprocessor.transformers_[1][1]
        .get_feature_names_out(categorical_cols)
        .tolist()
    )
    feature_prefixes = [x.split('_')[0] for x in categorical_feature_names]
    logger.info(f'Categorical features: {pprint.pformat(Counter(feature_prefixes))}')

    ann_index = hnswlib.Index(
        space='l2', dim=df_preprocessed.shape[1]
    )  # possible options are l2, cosine or ip

    # Initializing index
    # max_elements - the maximum number of elements (capacity). Will throw an exception if exceeded
    # during insertion of an element.
    # The capacity can be increased by saving/loading the index, see below.
    #
    # ef_construction - controls index search speed/build speed tradeoff
    #
    # M - is tightly connected with internal dimensionality of the data. Strongly affects memory consumption (~M)
    # Higher M leads to higher accuracy/run_time at fixed ef/efConstruction
    ann_index.init_index(
        max_elements=df_preprocessed.shape[0], ef_construction=60000, M=256
    )
    ann_index.add_items(df_preprocessed, diffs)

    # Controlling the recall by setting ef:
    # higher ef leads to better accuracy, but slower search
    # p.set_ef(10)

    # Query dataset, k - number of the closest elements (returns 2 numpy arrays)
    labels, distances = ann_index.knn_query(df_preprocessed, k=1)

    # Index objects support pickling
    # WARNING: serialization via pickle.dumps(p) or p.__getstate__() is NOT thread-safe with p.add_items method!
    # Note: ef parameter is included in serialization; random number generator is initialized with random_seed on Index load

    with open(index_path, 'wb') as file:
        file.write(pickle.dumps(ann_index))
    with open(preprocessor_path, 'wb') as file:
        file.write(pickle.dumps(preprocessor))

    ### Index parameters are exposed as class properties:
    logger.info(
        f'Index space={ann_index.space} dim={ann_index.dim} M={ann_index.M} '
        f'ef_construction={ann_index.ef_construction} elements {ann_index.element_count}/{ann_index.max_elements} '
    )

    return SimpleNamespace(ann_index=ann_index, preprocessor=preprocessor)
