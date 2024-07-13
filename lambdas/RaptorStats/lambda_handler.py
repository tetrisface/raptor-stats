import os
from RaptorStats import raptor_stats
from Common.logger import lambda_handler_decorator


@lambda_handler_decorator
def handler(event, context):
    os.environ['details_fetch_limit'] = event.get('details_fetch_limit', '500')
    return raptor_stats.main()


if __name__ == '__main__':
    handler(None, None)
