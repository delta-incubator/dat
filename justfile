# setup the local environment to develop dat
setup:
    uv sync
    uvx pre-commit install

# update pre-commit hooks
update-hooks:
    uvx pre-commit autoupdate

# run code linting
lint:
    uvx pre-commit run --all-files

# run unit tests
test:
    uv run --no-sync pytest tests

generate-tables:
    uv run dat generate-tables

generate-schemas:
    uv run dat generate-schemas
