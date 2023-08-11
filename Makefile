.PHONY: clean-env
clean-env:
	rm -rf .venv

.PHONY: setup-env
setup-env: clean-env
	poetry config virtualenvs.in-project true
	poetry install

.PHONY: tests
tests:
	poetry run pytest tests