import logging
import os
import sys


class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.relativeCreated = f'{round(record.relativeCreated):7,}'.replace(',', ' ')
        return super().format(record)


def lambda_handler_decorator(func):
    def wrapper(event, context):
        os.environ['LAMBDA_NAME'] = context.function_name
        logger = get_logger(context.function_name)
        logger.debug('event: %s', event)
        try:
            result = func(event, context)
        except Exception as e:
            logger.exception(e)
            raise e
        return result

    return wrapper


def get_logger(function_name=''):
    if os.environ.get('ENV', 'prod') == 'dev':
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.INFO,
        )

    _logger = logging.getLogger()

    _logger.setLevel(logging.INFO)
    function_name = (
        function_name if function_name else os.environ.get('LAMBDA_NAME', '')
    )
    function_name = function_name.ljust(12) if function_name else ''
    formatter = CustomFormatter(
        (
            f'{'' if function_name in _logger.handlers[0].formatter._fmt else function_name}{'' if '%(relativeCreated)s ' in _logger.handlers[0].formatter._fmt else '%(relativeCreated)s '}'
            + _logger.handlers[0].formatter._fmt
        )
        .replace(':%', ' %')
        .replace('%(name)s ', '')
        .replace('%(relativeCreated)s %(relativeCreated)s ', '%(relativeCreated)s ')
    )
    _logger.handlers[0].setFormatter(formatter)
    return _logger
