FROM public.ecr.aws/lambda/python:3.12

RUN pip install requests
RUN pip install pandas
RUN pip install boto3
RUN pip install gspread
RUN pip install pyarrow
RUN pip install s3fs
RUN pip install pytz


COPY aws_lambda ${LAMBDA_TASK_ROOT}

CMD [ "index.handler" ]
