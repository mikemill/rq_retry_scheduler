test:
	py.test --pep8 --flakes --cov=rq_retry_scheduler --cov-config .coveragerc

coverage-html:
	coverage html -d coverage_html
