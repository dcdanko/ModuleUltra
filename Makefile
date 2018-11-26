.PHONY: lint

lint:
	pylint --rcfile=.pylintrc moduleultra -f parseable -r n && \
	pycodestyle moduleultra --max-line-length=120 && \
	pydocstyle moduleultra
