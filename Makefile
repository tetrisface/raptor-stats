script:
	pipenv run jupyter nbconvert --to script raptor_stats.ipynb
	mv raptor_stats.py build/aws
	cp gamesettings.py build/aws

install:
	ROOTDIR=${shell pwd}
	pipenv install
	pipenv clean
	# pipenv run python -c "import site; print(site.getsitepackages()[0])"
	cd `pipenv run  python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])'`
	zip -r ${ROOTDIR}/build/aws/package.zip .
	cd "${ROOTDIR}/build/aws/"
	zip package.zip raptor_stats.py
	# aws cloudformation package --template-file raptor_stats.yaml --s3-bucket raptor-stats --output-template-file package.yaml

config:
	echo "yes"

setup:
	pipenv install
	bun install

run:
	pipenv run python ./build/aws/raptor_stats.py

requirements:
	# python
	asdf install
	pip install pipenv --user --upgrade


	# nodejs
	# curl -fsSL https://bun.sh/install | bash
	# bun install -g aws-cdk
	# bun install -g ts-node
	bun install

	# apt install zip # aws?
