test:
	py.test --flake8 --cov=rq_retry_scheduler --cov-config .coveragerc

coverage-html:
	coverage html -d coverage_html

install:
	python setup.py install --record files.txt

uninstall:
	echo "files.txt" | cat - files.txt | xargs rm
	rm -r dist build rq_retry_scheduler.egg-info

reinstall: uninstall install
