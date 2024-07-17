import os
from typing import Literal
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


def number_to_column_letter(n):
    result = ''
    while n >= 0:
        n, remainder = divmod(n, 26)
        result = chr(remainder + 65) + result
        n -= 1
    return result


def main(event):
    _id = event['id']
    sheet_name = event['sheet_name']
    column_rows = event['columns']
    parquet_path = event['parquet_path']
    batch_requests = event['batch_requests']
    clear = event['clear']
    notes = event['notes']

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
        values=column_rows + df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    logger.info(f'clearing notes {len(column_rows[0])} columns')
    if len(column_rows) > 0:
        worksheet.clear_note(1, 1, len(column_rows), len(column_rows[0]))

    if len(notes) > 0:
        worksheet.update_notes(notes)

    if len(batch_requests) > 0:
        spreadsheet.batch_update({'requests': batch_requests})
