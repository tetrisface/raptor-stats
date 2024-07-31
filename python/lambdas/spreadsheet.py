import os

import gspread
import orjson
from common.common import get_secret, s3_download_df
from common.logger import get_logger, lambda_handler_decorator

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


@lambda_handler_decorator
def main(*args):
    event = args[0]
    _id = event['id']
    sheet_name = event['sheet_name']
    column_rows = event['columns']
    parquet_bucket = event['parquet_bucket']
    parquet_key = event['parquet_key']
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
            logger.info('Failed connection to google, stopping')
            return 'failed'

    worksheet = get_or_create_worksheet(spreadsheet, sheet_name)

    if clear:
        logger.info('Clearing sheet')
        worksheet.clear()
        worksheet.update(
            values=[['UPDATE IN PROGRESS']],
            value_input_option=gspread.utils.ValueInputOption.user_entered,
        )

    df = s3_download_df(parquet_bucket, parquet_key)

    logger.info(f'pushing {len(df)} to {sheet_name}')
    worksheet.update(
        values=column_rows + df.rows(),
        value_input_option=gspread.utils.ValueInputOption.user_entered,
    )

    logger.info(f'clearing notes {len(column_rows[0])} columns')
    if len(column_rows) > 0:
        try:
            worksheet.clear_note(1, 1, len(column_rows), len(column_rows[0]))
        except gspread.exceptions.APIError as e:
            logger.exception(e)

    if len(notes) > 0:
        try:
            worksheet.update_notes(notes)
        except gspread.exceptions.APIError as e:
            logger.exception(e)

    if len(batch_requests) > 0:
        spreadsheet.batch_update({'requests': batch_requests})
