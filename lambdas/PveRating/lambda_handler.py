from PveRating import pve_rating
from Common.logger import lambda_handler_decorator


@lambda_handler_decorator
def handler(event, context):
    pve_rating.main()


if __name__ == '__main__':
    handler(None, None)
