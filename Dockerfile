FROM public.ecr.aws/lambda/python:3.12

RUN pip install requests
RUN pip install boto3
RUN pip install gspread
RUN pip install s3fs
RUN pip install pytz
RUN pip install polars==0.20.29
RUN pip install scikit-learn


COPY lambdas ${LAMBDA_TASK_ROOT}
