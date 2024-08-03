import os
import random
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.discriminant_analysis import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from common.logger import get_logger

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401

logger = get_logger()


def grouped_gamesettings_preprocessor(numerical_cols, categorical_cols):
    n_features = len(numerical_cols) + len(categorical_cols)
    logger.info(f'n features {n_features}')

    # Preprocessing for numerical data
    numerical_transformer = Pipeline(
        memory=None,
        steps=[
            ('imputer', SimpleImputer(strategy='mean')),
            ('scaler', StandardScaler()),
            (
                'pca',
                PCA(
                    n_components=14,
                    random_state=random.seed(),
                ),
            ),
        ],
    )

    # Preprocessing for categorical data
    categorical_transformer = Pipeline(
        memory=None,
        steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(handle_unknown='ignore')),
        ],
    )

    # Combine preprocessing steps
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_cols),
            ('cat', categorical_transformer, categorical_cols),
        ]
    )
    return preprocessor
