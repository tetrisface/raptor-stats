FROM public.ecr.aws/lambda/python:3.12

RUN pip install requests boto3 gspread s3fs pytz polars numpy orjson psutil

COPY lambdas ${LAMBDA_TASK_ROOT}
