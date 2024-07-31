DATA_BUCKET := s3://replays-processing/
LOCAL_PATH := var/

.PHONY: requirements setup notebook-to-py run-dev run install install-run tail upload download backup

requirements:
	asdf install # https://asdf-vm.com/guide/getting-started.html
	# pip install pipenv --user --upgrade
	sudo -H pip install pipenv --user --upgrade # works in wsl
	pipenv install

	curl -fsSL https://bun.sh/install | bash
	bun install -g aws-cdk ts-node @vue/cli
	bun install

	apt install zip awscli

setup:
	pipenv install
	bun install

run:
	(cd python && PIPENV_VERBOSITY=-1 pipenv run python -m scripts.invoke)

migrate:
	# download
	(cd python && PIPENV_VERBOSITY=-1 ENV=dev DATA_BUCKET=$(DATA_BUCKET) pipenv run python -m scripts.migrate)
	# backup
	# upload

deploy-get-requirements:
	echo "todo"

deploy:
	rm -rf cdk.out/* & ./clear_old_docker_images.sh
	(cd app && bun run build)
	cdk deploy --all

deploy-lambda:
	rm -rf cdk.out/*
	cdk deploy RaptorStats

deploy-app:
	rm -rf cdk.out/*
	(cd app && bun run build)
	cdk deploy WebAppStack

tail:
	aws logs tail /aws/lambda/RaptorStats --follow & aws logs tail /aws/lambda/PveRating --follow & aws logs tail /aws/lambda/Spreadsheet --follow

upload:
	aws s3 cp $(LOCAL_PATH)replays.parquet $(DATA_BUCKET)replays.parquet
	aws s3 cp $(LOCAL_PATH)replays_gamesettings.parquet $(DATA_BUCKET)replays_gamesettings.parquet

dl: download
download:
	aws s3 cp $(DATA_BUCKET)replays.parquet $(LOCAL_PATH)
	aws s3 cp $(DATA_BUCKET)replays_gamesettings.parquet $(LOCAL_PATH)

backup:
	aws s3 cp $(DATA_BUCKET)replays.parquet $(DATA_BUCKET)replays.parquet.backup.`date +%d`
	aws s3 cp $(DATA_BUCKET)replays_gamesettings.parquet $(DATA_BUCKET)replays_gamesettings.parquet.backup.`date +%d`

update:
	bunx npm-check-updates -i
	pipenv update
	bun update -g aws-cdk ts-node
