import logging
import os
import sys

import resource
import psutil


class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.relativeCreated = f'{round(record.relativeCreated):7,}'.replace(',', ' ')
        record.memUse = f'{psutil.Process(os.getpid()).memory_info().rss / 1024 ** 3:.2f} {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024**2 :.2f}'
        return super().format(record)


def lambda_handler_decorator(func):
    def wrapper(*args):
        event = args[0] if len(args) > 0 else None
        context = args[1] if len(args) > 1 else None
        os.environ['LAMBDA_NAME'] = (
            context.function_name if hasattr(context, 'function_name') else ''
        )
        logger = get_logger(os.environ['LAMBDA_NAME'])

        os.environ['details_fetch_limit'] = event.get('details_fetch_limit', '500')
        logger.debug('event: %s', event)
        try:
            result = func(event, context)
        except Exception as e:
            logger.exception(e)
            raise e
        return result

    return wrapper


def get_logger(function_name=''):
    log_level_str = os.environ.get('LOG_LEVEL', 'INFO').upper()
    log_level_str = 'INFO' if log_level_str.startswith('I') else log_level_str
    log_level = getattr(
        logging,
        'DEBUG' if log_level_str.startswith('D') else log_level_str,
    )

    if os.environ.get('ENV', 'prod') == 'dev':
        logging.basicConfig(
            stream=sys.stdout,
            level=log_level,
        )

    _logger = logging.getLogger()
    _logger.setLevel(log_level)
    if len(_logger.handlers) == 0:
        return _logger

    function_name = (
        function_name if function_name else os.environ.get('LAMBDA_NAME', '')
    )
    function_name = function_name.ljust(12) if function_name else ''

    fmt = _logger.handlers[0].formatter._fmt
    formatter = CustomFormatter(
        (
            f'{'' if function_name in fmt else function_name}{'' if '%(relativeCreated)s ' in fmt else '%(relativeCreated)s '}{'' if '💾' in fmt else '💾 %(memUse)s '}'
            + fmt
        )
        .replace(':%', ' %')
        .replace('%(name)s ', '')
        .replace('%(relativeCreated)s %(relativeCreated)s ', '%(relativeCreated)s ')
    )
    _logger.handlers[0].setFormatter(formatter)
    return _logger
