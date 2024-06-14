FROM public.ecr.aws/lambda/python:3.12

RUN pip install requests
RUN pip install boto3
RUN pip install gspread
RUN pip install s3fs
RUN pip install pytz
RUN pip install polars
RUN pip install numpy
RUN pip install orjson

COPY lambdas ${LAMBDA_TASK_ROOT}
