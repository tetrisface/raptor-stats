import logging
import os
from RaptorStats import raptor_stats

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if os.environ.get('ENV', 'prod') == 'dev':
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def handler(event, context):
    try:
        logger.info('event: %s', event)
        os.environ['details_fetch_limit'] = event.get('details_fetch_limit', '20')
        return raptor_stats.main()
    except Exception as e:
        logger.exception(e)
        raise e


if __name__ == '__main__':
    handler(None, None)
