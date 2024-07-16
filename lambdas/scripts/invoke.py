import boto3
import orjson

lambda_client = boto3.client('lambda')
lambda_client.invoke(
    # FunctionName='RaptorStats',
    FunctionName='PveRating',
    InvocationType='Event',
    # Payload=orjson.dumps({'details_fetch_limit': '5'}),
)
