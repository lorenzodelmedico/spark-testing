
init_env : init_virtualenv load_direnv install precommit_install
	@echo "✅ Environment initialized and ready to use 🔥"

init_virtualenv :
	@echo "Initializing environment ..."
	@if pyenv virtualenvs | grep -q 'sparktest'; then \
		echo "Virtualenv 'sparktest' already exists"; \
	else \
		echo "Virtualenv 'sparktest' does not exist"; \
		echo "Creating virtualenv 'sparktest' ..."; \
		pyenv virtualenv 3.10.12 sparktest; \
	fi
	@pyenv local sparktest
	@echo "✅ Virtualenv 'sparktest' activated"

load_direnv:
	@echo "Loading direnv ..."
	@direnv allow
	@echo "✅ Direnv loaded"

precommit_install:
	@echo "Installing pre-commit hooks ..."
	@pre-commit install
	@echo "✅ Pre-commit hooks installed"

install :
	@echo "Installing dependencies ..."
	@pip install --upgrade -q pip
	@pip install -q -r requirements.txt
	@echo "✅ Dependencies installed"
# @echo "Installing local package sparktest ..."
# @tree src
# @pip install -q -e .



.PHONY: tests

# Run tests using pytest
tests:
	pytest

