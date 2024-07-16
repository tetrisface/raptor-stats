import os
import warnings

import gspread
import orjson
import polars as pl
import s3fs
from Common.common import get_secret
from Common.logger import get_logger

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401


def get_or_create_worksheet(spreadsheet, sheet_name, rows=1, cols=1, index=0):
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=rows,
            cols=cols,
            index=index,
        )
    return worksheet


def main(event):
    _id = event['id']
    sheet_name = event['sheet_name']
    columns = event['columns']
    parquet_path = event['parquet_path']
    batch_requests = event['batch_requests']
    clear = event['clear']

    if dev:
        gc = gspread.service_account()
        spreadsheet = gc.open_by_key(_id)
    else:
        try:
            gc = gspread.service_account_from_dict(orjson.loads(get_secret()))
            spreadsheet = gc.open_by_key(_id)
        except gspread.exceptions.APIError as e:
            logger.exception(e)
            logger.info('failed connection to google, stopping')
            return 'failed'

    worksheet = get_or_create_worksheet(spreadsheet, sheet_name)

    if clear:
        logger.info('clearing sheet')
        worksheet.clear()
        worksheet.update(
            values=[['UPDATE IN PROGRESS']],
            value_input_option=gspread.utils.ValueInputOption.user_entered,
        )

    fs = s3fs.S3FileSystem()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=UserWarning)

        with fs.open(parquet_path, mode='rb') as f:
            df = pl.read_parquet(f)

    logger.info(f'pushing {len(df)} to {sheet_name}')
    worksheet.update(
        values=columns + df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    if len(batch_requests) > 0:
        spreadsheet.batch_update({'requests': batch_requests})
