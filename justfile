# setup local pre-commit hooks
setup:
    uvx pre-commit install

# update pre-commit hooks
update-hooks:
    uvx pre-commit autoupdate

# run code linting
lint:
    uvx pre-commit run --all-files

# run workload-generator tests
test:
    cd workload-generator && sbt test

generate-workloads output="/tmp/dat-workloads":
    cd workload-generator && WORKLOAD_OUTPUT_DIR={{output}} sbt "testOnly io.delta.workload.tables.*"
