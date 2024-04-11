.PHONY: requirements setup notebook-to-py run-dev-local run install install-run tail

requirements:
	asdf install
	pip install pipenv --user --upgrade
	pipenv install

	# curl -fsSL https://bun.sh/install | bash
	# bun install -g aws-cdk
	# bun install -g ts-node ?
	bun install

	# apt install zip awscli

setup:
	pipenv install
	bun install

notebook-to-py:
	pipenv run jupyter nbconvert --to python raptor_stats.ipynb --output aws_lambda/raptor_stats.py

run:
	aws lambda invoke --function-name arn:aws:lambda:eu-north-1:190920611368:function:RaptorStats /dev/stdout

run-dev:
	PIPENV_IGNORE_VIRTUALENVS=1 ENV=dev pipenv run python aws_lambda/index.py

install:
	cdk deploy

deploy: install

install-run:
	make install && make run

tail:
	aws logs tail /aws/lambda/RaptorStats --follow

upload:
	aws s3 cp replays.parquet s3://raptor-stats-parquet/replays.parquet
	aws s3 cp replays_gamesettings.parquet s3://raptor-stats-parquet/replays_gamesettings.parquet

download:
	aws s3 cp s3://raptor-stats-parquet/replays.parquet .
	aws s3 cp s3://raptor-stats-parquet/replays_gamesettings.parquet .
