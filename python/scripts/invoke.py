from common.common import invoke_lambda

invoke_lambda('RaptorStats', {'details_fetch_limit': '100'})
# invoke_lambda('PveRating')
# invoke_lambda('RecentGames')
