import logging
import raptor_stats

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.debug("event: %s", event)
    try:
        raptor_stats.main()
    except Exception as e:
        logger.exception(e)
    return


if __name__ == "__main__":
    handler(None, None)
