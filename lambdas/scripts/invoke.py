import boto3

lambda_client = boto3.client('lambda')
lambda_client.invoke(FunctionName='RaptorStats', InvocationType='Event')
