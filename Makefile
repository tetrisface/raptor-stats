DATA_BUCKET := s3://replays-processing/

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
	# aws lambda invoke --function-name arn:aws:lambda:eu-north-1:190920611368:function:RaptorStats /dev/stdout
	(cd lambdas && PIPENV_VERBOSITY=-1 ENV=dev pipenv run python -m scripts.invoke)

run-dev:
	(cd lambdas && PIPENV_VERBOSITY=-1 ENV=dev pipenv run python -m PveRating.pve_rating)

run-fetch-dev:
	(cd lambdas && PIPENV_VERBOSITY=-1 ENV=dev pipenv run python -m RaptorStats.raptor_stats)

migrate:
	make download
	(cd lambdas && PIPENV_VERBOSITY=-1 ENV=dev pipenv run python -m scripts.migrate)
	make upload

install:
	rm -rf cdk.out/*
	cdk deploy --all

deploy: install

install-run:
	make install && make run

tail:
	aws logs tail /aws/lambda/RaptorStats --follow & aws logs tail /aws/lambda/PveRating --follow & aws logs tail /aws/lambda/Spreadsheet --follow

upload:
	aws s3 cp lambdas/replays.parquet $(DATA_BUCKET)replays.parquet
	aws s3 cp lambdas/replays_gamesettings.parquet $(DATA_BUCKET)replays_gamesettings.parquet

download:
	aws s3 cp $(DATA_BUCKET)replays.parquet lambdas/
	aws s3 cp $(DATA_BUCKET)replays_gamesettings.parquet lambdas/

backup:
	aws s3 cp $(DATA_BUCKET)replays.parquet $(DATA_BUCKET)replays.parquet.backup.`date +%d`
	aws s3 cp $(DATA_BUCKET)replays_gamesettings.parquet $(DATA_BUCKET)replays_gamesettings.parquet.backup.`date +%d`

update:
	bunx npm-check-updates -i
	pipenv update
	bun install -g aws-cdk ts-node
