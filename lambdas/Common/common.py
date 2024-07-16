import os
import boto3
import polars as pl

from Common.logger import get_logger

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'


replay_root_file_name = 'replays.parquet'
replay_details_file_name = 'replays_gamesettings.parquet'
READ_DATA_BUCKET = os.environ.get('READ_DATA_BUCKET', os.environ['DATA_BUCKET'])
WRITE_DATA_BUCKET = os.environ.get('WRITE_DATA_BUCKET', os.environ['DATA_BUCKET'])


def get_df_s3(path):
    s3_path = os.path.join(READ_DATA_BUCKET, path)
    logger.info(f'fetching from "{s3_path}"')
    _df = pl.read_parquet(s3_path)
    if dev:
        logger.info(f'writing {len(_df)} to {path}')
        _df.write_parquet(path)
    return _df


def get_df(path):
    if dev and not os.path.exists(path):
        return get_df_s3(path)
    if not dev:
        path = os.path.join(READ_DATA_BUCKET, path)
    df = pl.read_parquet(path)
    logger.info(f'fetched {len(df)} from "{path}"')
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
