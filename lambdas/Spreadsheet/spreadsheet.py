import os

import gspread
import orjson
import polars as pl
from Common.common import get_secret
from Common.logger import get_logger

logger = get_logger()

dev = os.environ.get('ENV', 'prod') == 'dev'
if dev:
    from bpdb import set_trace as s  # noqa: F401


def main(event):
    _id = event['id']
    sheet_name = event['sheet_name']
    columns = event['columns']
    parquet_file_name = event['parquet_file_name']
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

    worksheet = spreadsheet.worksheet(sheet_name)

    if clear:
        logger.info('clearing sheet')
        worksheet.clear()
        worksheet.update(
            values=[['UPDATE IN PROGRESS']],
            value_input_option=gspread.utils.ValueInputOption.user_entered,
        )

    if dev:
        df = pl.read_parquet(parquet_file_name)
    else:
        df = pl.read_parquet(
            f's3://raptor-stats-parquet/spreadsheets/{parquet_file_name}'
        )

    logger.info(f'pushing {len(df)} to {sheet_name}')
    worksheet.update(
        values=columns + df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    if len(batch_requests) > 0:
        spreadsheet.batch_update({'requests': batch_requests})


if __name__ == '__main__':
    main({})
