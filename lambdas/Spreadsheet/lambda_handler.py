from Spreadsheet import spreadsheet
from Common.logger import lambda_handler_decorator


@lambda_handler_decorator
def handler(event, context):
    return spreadsheet.main(event)


if __name__ == '__main__':
    handler(None, None)
