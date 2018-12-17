test:
	py.test --cov=rq_retry_scheduler --cov-config .coveragerc
	git ls-files '*.py' | xargs flake8

coverage-html:
	coverage html -d coverage_html

install:
	python setup.py install --record files.txt

uninstall:
	echo "files.txt" | cat - files.txt | xargs rm
	rm -r dist build rq_retry_scheduler.egg-info

reinstall: uninstall install

build: rq_retry_scheduler/*.py rq_retry_scheduler/cli/*.py
	bin/python setup.py sdist bdist_wheel --universal

upload: build
	bin/twine upload dist/*

upload-test: build
	bin/twine upload --repository-url https://test.pypi.org/legacy/ dist/*
