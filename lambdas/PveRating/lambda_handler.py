import logging
import os
from PveRating import pve_rating

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if os.environ.get('ENV', 'prod') == 'dev':
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def handler(event, context):
    logger.debug('event: %s', event)
    try:
        pve_rating.main()
    except Exception as e:
        logger.exception(e)
        raise e


if __name__ == '__main__':
    handler(None, None)
