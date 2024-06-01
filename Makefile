.PHONY: requirements setup notebook-to-py run-dev run install install-run tail upload download backup

requirements:
	asdf install # https://asdf-vm.com/guide/getting-started.html
	# pip install pipenv --user --upgrade
	sudo -H pip install pipenv --user --upgrade
	pipenv install

	curl -fsSL https://bun.sh/install | bash
	bun install -g aws-cdk ts-node
	bun install

	apt install zip awscli

setup:
	pipenv install
	bun install

notebook-to-py:
	pipenv run jupyter nbconvert --to python raptor_stats.ipynb --output aws_lambda/raptor_stats.py

run:
	aws lambda invoke --function-name arn:aws:lambda:eu-north-1:190920611368:function:RaptorStats /dev/stdout

run-dev:
	PIPENV_VERBOSITY=-1 ENV=dev pipenv run python aws_lambda/index.py

install:
	make backup
	cdk deploy

deploy: install

install-run:
	make install && make run

tail-stats:
	aws logs tail /aws/lambda/RaptorStats --follow

tail-skill:
	aws logs tail /aws/lambda/PveSkill --follow

upload:
	aws s3 cp lambdas/replays.parquet s3://raptor-stats-parquet/replays.parquet
	aws s3 cp lambdas/replays_gamesettings.parquet s3://raptor-stats-parquet/replays_gamesettings.parquet

download:
	aws s3 cp s3://raptor-stats-parquet/replays.parquet lambdas/
	aws s3 cp s3://raptor-stats-parquet/replays_gamesettings.parquet lambdas/

backup:
	aws s3 cp s3://raptor-stats-parquet/replays.parquet s3://raptor-stats-parquet/replays.parquet.backup.`date +%d`
	aws s3 cp s3://raptor-stats-parquet/replays_gamesettings.parquet s3://raptor-stats-parquet/replays_gamesettings.parquet.backup.`date +%d`
