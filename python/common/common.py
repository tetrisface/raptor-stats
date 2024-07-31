import importlib
import os
from pathlib import Path
import re
import tempfile
import warnings
import boto3
import orjson
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.discriminant_analysis import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from common.logger import get_logger
from bpdb import set_trace as s

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'


replay_root_file_name = 'replays.parquet'
replay_details_file_name = 'replays_gamesettings.parquet'
READ_DATA_BUCKET = os.environ.get('READ_DATA_BUCKET', os.environ.get('DATA_BUCKET'))
WRITE_DATA_BUCKET = os.environ.get('WRITE_DATA_BUCKET', os.environ.get('DATA_BUCKET'))
FILE_SERVE_BUCKET = os.environ.get('FILE_SERVE_BUCKET', 'pve-rating-web-file-serve')
LOCAL_DATA_DIR = os.environ.get(
    'LOCAL_DATA_DIR', os.path.join(Path(os.getcwd()).parent, 'var')
)


def get_df(path):
    if dev and not os.path.exists(path):
        df = s3_download_df(READ_DATA_BUCKET, path)
        df.write_parquet(path)
        return df
    if not dev:
        path = os.path.join('s3://', READ_DATA_BUCKET, path)
    df = pl.read_parquet(path)
    logger.info(f'Fetched {len(df)} from "{path}"')
    return df


def get_secret():
    secret_name = 'raptor-gcp'
    region_name = 'eu-north-1'

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise e

    return get_secret_value_response['SecretString']


def interpolate(value, in_min, in_max, out_min, out_max):
    # Clamp the value within the input range
    value = max(in_min, min(value, in_max))

    # Calculate the interpolation factor
    t = (value - in_min) / (in_max - in_min)

    # Calculate and return the interpolated result
    return out_min + t * (out_max - out_min)


def invoke_lambda(function_name: str, payload: dict = {}):
    if dev:
        logger.info(f'Invoking local {function_name} {payload.get('sheet_name', '')}')
        module_name = os.path.join(
            'lambdas', re.sub(r'(?<!^)(?=[A-Z])', '_', function_name).lower()
        )
        spec = importlib.util.spec_from_file_location(module_name, module_name + '.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        module.main(payload, {'function_name': function_name})
    else:
        logger.info(f'Invoking {function_name} {payload.get('sheet_name', '')}')
        boto3.client('lambda').invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=orjson.dumps(payload),
        )


def s3_upload_df(df, bucket, key):
    if not bucket:
        key = os.path.join(LOCAL_DATA_DIR, key)
        Path(os.path.dirname(key)).mkdir(parents=True, exist_ok=True)
        logger.info(f'Writing {len(df)} locally to {key}')
        df.write_parquet(key)
        return

    logger.info(f'Uploading {len(df)} to s3://{bucket}/{key}')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=UserWarning)
        with tempfile.SpooledTemporaryFile() as tmp_file:
            df.write_parquet(tmp_file)
            tmp_file.seek(0)
            boto3.client('s3').upload_fileobj(
                tmp_file,
                bucket + ('-dev' if dev else ''),
                key,
                ExtraArgs={'StorageClass': 'INTELLIGENT_TIERING'},
            )


def s3_download_df(bucket, key):
    if not bucket:
        key = os.path.join(LOCAL_DATA_DIR, key)
        df = pl.read_parquet(key)
        logger.info(f'Read {len(df)} locally from {key}')
        return df
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=UserWarning)
        with tempfile.SpooledTemporaryFile() as tmp_file:
            boto3.client('s3').download_fileobj(
                bucket + ('-dev' if dev else ''),
                key,
                tmp_file,
            )
            tmp_file.seek(0)
            df = pl.read_parquet(tmp_file)
    return df


def grouped_gamesettings_preprocessor(numerical_cols, categorical_cols):
    # Preprocessing for numerical data
    numerical_transformer = Pipeline(
        memory=None,
        steps=[
            ('imputer', SimpleImputer(strategy='mean')),
            ('scaler', StandardScaler()),
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
