

POETRY_RUN := poetry run
BLUE=\033[0;34m
NC=\033[0m # No Color
PROJ=dat

.PHONY: update install help autolint lint-mypy lint-base lint report-coverage lint-bandit lint-flake8 test

autolint: ## Autolint the code
	@echo "\n${BLUE}Running autolinting...${NC}\n"
	@${POETRY_RUN} autopep8 ${PROJ}/* tests/* -r -i --experimental
	@${POETRY_RUN} isort ${PROJ} tests/
	@${POETRY_RUN} unify -r -i ${PROJ} tests/

lint-mypy: ## Just run mypy
	@echo "\n${BLUE}Running mypy...${NC}\n"
	@${POETRY_RUN} mypy --show-error-codes ${PROJ}

lint-flake8: ## Run the flake8 linter
	@echo "\n${BLUE}Running flake8...${NC}\n"
	@${POETRY_RUN} flake8 ${PROJ} tests/

lint-bandit: ## Run bandit
	@echo "\n${BLUE}Running bandit...${NC}\n"
	@${POETRY_RUN} bandit -r ${PROJ}

lint-base: lint-flake8 lint-bandit ## Just run the linters without autolinting

lint: autolint lint-base lint-mypy ## Autolint and code linting

install: ## Install Python Dependencies via poetry
	@echo "\n${BLUE}Installing dependencies${NC}\n"
	poetry install

test-only: ## Run a single test like so make test_name='tests/test_tables.py' test-only
	@${POETRY_RUN} python -m pytest --durations=0 -v $(test_name)

test: ## Run all the tests with code coverage.
	@echo "\n${BLUE}Running pytest with coverage...${NC}\n"
	@${POETRY_RUN} coverage erase;
	@${POETRY_RUN} coverage run --branch -m pytest tests \
		--junitxml=junit/test-results.xml \
		--doctest-modules -vv --capture=no


report-coverage: ## Report coverage
	@${POETRY_RUN} coverage report
	@${POETRY_RUN} coverage html
	@${POETRY_RUN} coverage xml


write-generated-tables:
	@${POETRY_RUN} python -m dat.main write-generated-reference-tables

write-model-schemas:
	@${POETRY_RUN} python -m dat.main write-model-schemas

write-all: write-generated-tables write-model-schemas

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; \
		{printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

reset-generated-tables:
	git rm -rf out/tables/generated --ignore-unmatch
