import logging
import os
import raptor_stats

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if os.environ.get("ENV", "prod") == "dev":
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def handler(event, context):
    logger.debug("event: %s", event)
    try:
        return raptor_stats.main()
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    handler(None, None)
