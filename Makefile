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

.PHONY: format
format:
	poetry run black pyspark_confluent_avro tests
	poetry run ruff check --fix pyspark_confluent_avro tests

.PHONY: check
check:
	poetry run black --check pyspark_confluent_avro tests
	poetry run ruff check pyspark_confluent_avro tests
	poetry run mypy pyspark_confluent_avro tests