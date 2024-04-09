.PHONY: requirements setup notebook-to-py run-dev-local run install install-run tail

requirements:
	asdf install
	pip install pipenv --user --upgrade

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

run-dev-local:
	ENV=dev pipenv run python ./build/aws/raptor_stats.py

run:
	aws lambda invoke --function-name arn:aws:lambda:eu-north-1:190920611368:function:RaptorStats /dev/stdout

run-dev:
	ENV=dev make run

install:
	cdk deploy

install-run:
	make install && make run

tail:
	aws logs tail /aws/lambda/RaptorStats --follow
