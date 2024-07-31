FROM public.ecr.aws/lambda/python:3.12

RUN pip install requests boto3 gspread pytz polars numpy orjson psutil

COPY python ${LAMBDA_TASK_ROOT}
