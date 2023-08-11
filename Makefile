clean-env:
	rm -rf .venv

setup-env: clean-env
	poetry config virtualenvs.in-project true
	poetry install