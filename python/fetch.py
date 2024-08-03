import datetime
import os

import requests
import boto3

dev = os.environ.get('ENV', 'prod') == 'dev'

if dev:
    from bpdb import set_trace as s  # noqa: F401


def query_by_date_and_ids(date: str, ids: list[str]) -> list[dict]:
    with boto3.client('dynamodb') as client:
        id_parameter_values = {f':id{i}': id for i, id in enumerate(ids)}
        params = {
            'TableName': 'replays',
            'KeyConditionExpression': f'date = :date AND id IN ({','.join(id_parameter_values.keys())})',
            'ExpressionAttributeValues': {
                ':date': {'S': date},
                **id_parameter_values,
            },
        }

        response = client.query(**params)
        return response.get('Items', [])


def main():
    # fetch 2 pages of list api with delay
    # if both of them have new data continue to fetch until they don't
    ids = fetch_api_root()
    # fetch details api with delay for all new ids
    data = fetch_api_details(ids)

    # insert new data to dynamodb
    # invoke fetch_merge

    # date_queries = [('2024-07-28', ['id1', 'id2']), ('2024-07-29', ['id3', 'id4'])]

    # Create a list of tasks
    tasks = [query_by_date_and_ids(date, ids) for date, ids in date_queries]

    # Combine results
    combined_results = []
    for result in tasks:
        combined_results.extend(result)

    print(combined_results)


def fetch_api_root():
    n_received_rows = page_size = int(
        os.environ.get('LIST_PAGE_SIZE', 10 if dev else 100)
    )
    page = int(os.environ.get('LIST_PAGE_START', 1))
    n_total_received_rows = 0
    api_min_date_fetched = datetime.datetime.now(datetime.timezone.utc)
    update = bool(os.environ.get('LIST_PAGE_UPDATE', False))
    list_page_page_limit = int(os.environ.get('LIST_PAGE_PAGE_LIMIT', 1 if dev else 50))
    while (
        n_received_rows > 1
        and page_size > 0
        and page <= list_page_page_limit
        and api_min_date_fetched
        >= datetime.datetime.fromisoformat(
            os.environ.get('LIST_PAGE_DATE_LIMIT', '2024-04-01')
        ).replace(tzinfo=datetime.timezone.utc)
    ):
        apiUrl = f'https://api.bar-rts.com/replays?limit={page_size}&hasBots=true&page={page}'
        if page > 1:
            time.sleep(0.4)
        logger.info(
            f'fetching {apiUrl} received {n_received_rows}/{n_total_received_rows} page {page}/{list_page_page_limit} date {api_min_date_fetched}'
        )
        replays_json = requests.get(
            apiUrl,
            headers={'User-Agent': os.environ['DISCORD_USERNAME']},
        ).json()

        data = replays_json['data']

        api = (
            pl.DataFrame(data)
            .with_columns(
                pl.col('Map')
                .struct.field('scriptName')
                .str.replace(
                    r'(?i)[_\s]+[v\d\.]+\w*$',
                    '',
                )
                .alias('Map Name'),
                pl.col('startTime').str.to_datetime(
                    '%+', time_unit='ns', time_zone='UTC', strict=True, exact=True
                ),
            )
            .drop('Map')
            .filter(True if update else ~pl.col('id').is_in(games['id'].to_list()))
        )

        games = games.with_columns(
            [pl.lit(None).alias(x) for x in set(api.columns) - set(games.columns)]
        )

        n_received_rows = len(api)
        n_total_received_rows += n_received_rows
        n_before_games = len(games)
        api_min_date_fetched = api['startTime'].min()

        if api['startTime'].dtype != pl.Datetime:
            api = api.with_columns(
                startTime=pl.col('startTime').str.to_datetime('%+', time_unit='ns')
            )
        if update:
            games = games.update(
                api['id', 'Map Name'],
                how='left',
                on='id',
            )
        else:
            games = pl.concat(
                [
                    games,
                    api['startTime', 'durationMs', 'AllyTeams', 'id', 'Map Name'],
                ],
                how='vertical_relaxed',
            )
        logger.info(f'Games {n_before_games} + {n_received_rows} = {len(games)}')
        page += 1


if __name__ == '__main__':
    main()
