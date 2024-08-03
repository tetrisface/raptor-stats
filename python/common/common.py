import importlib
import os
import re
import tempfile
import warnings
from pathlib import Path

import boto3
import orjson
import polars as pl
from common.logger import get_logger

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'

if dev:
    from bpdb import set_trace as s  # noqa: F401


replay_root_file_name = 'replays.parquet'
replay_details_file_name = 'replays_gamesettings.parquet'
READ_DATA_BUCKET = os.environ.get('READ_DATA_BUCKET', os.environ.get('DATA_BUCKET'))
WRITE_DATA_BUCKET = os.environ.get('WRITE_DATA_BUCKET', os.environ.get('DATA_BUCKET'))
FILE_SERVE_BUCKET = os.environ.get('FILE_SERVE_BUCKET', 'pve-rating-web-file-serve')
LOCAL_DATA_DIR = os.environ.get(
    'LOCAL_DATA_DIR', os.path.join(Path(os.getcwd()).parent, 'var')
)


def get_secret():
    secret_name = 'raptor-gcp'
    region_name = 'eu-north-1'

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

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
            re.sub(r'(?<!^)(?=[A-Z])', '_', function_name).lower()
        )
        module = importlib.import_module('.', module_name)
        if hasattr(module, 'main'):
            module.main(payload, {'function_name': function_name})
        else:
            logger.error(f'Failed importing {module_name}.py (no main())')

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
                bucket,
                key,
                ExtraArgs={'StorageClass': 'INTELLIGENT_TIERING'},
            )


def s3_download_df(bucket, key):
    if not bucket:
        key = os.path.join(LOCAL_DATA_DIR, key)
        df = pl.read_parquet(key)
        logger.info(f'Read {len(df)} locally from {key}')
        return df

    path = os.path.join(LOCAL_DATA_DIR, key)
    if os.environ.get('LOCAL_CACHE') and os.path.exists(path):
        df = pl.read_parquet(path)
        logger.info(f'Read {len(df)} locally from {key}, reason=LOCAL_CACHE')
        return df

    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            with tempfile.SpooledTemporaryFile() as tmp_file:
                boto3.client('s3').download_fileobj(
                    bucket,
                    key,
                    tmp_file,
                )
                tmp_file.seek(0)
                df = pl.read_parquet(tmp_file)
    except Exception as e:
        logger.error(e)
        logger.info(f'Failed fetching s3://{bucket}/{key}')
        raise
    logger.info(f'Fetched {len(df)} from s3://{bucket}/{key}')
    return df


def user_ids_name_map(games):
    logger.info('Making user id->player name mapping')
    cols = (
        games.sort('startTime', descending=False)
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
        .to_dict()
    )
    return {k: v for (k, v) in zip(cols['userId'], cols['name'])}
