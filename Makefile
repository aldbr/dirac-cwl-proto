# Makefile for DIRAC CWL Prototype Schema Management

.PHONY: help schemas schemas-json schemas-yaml clean-schemas test-schemas install lint test

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package and dependencies
	pip install -e .
	pip install -e .[testing]

schemas: schemas-json schemas-yaml  ## Generate all schemas (JSON and YAML)

schemas-json:  ## Generate JSON schemas from Pydantic models
	@echo "Generating JSON schemas..."
	python scripts/generate_schemas.py \
		--output-dir generated_schemas \
		--format json \
		--individual \
		--unified
	@echo "Copying schemas to test locations..."
	cp generated_schemas/dirac-metadata.json test/workflows/test_meta/schemas/dirac-metadata.json

schemas-yaml:  ## Generate YAML schemas from Pydantic models
	@echo "Generating YAML schemas..."
	python scripts/generate_schemas.py \
		--output-dir generated_schemas \
		--format yaml \
		--unified

clean-schemas:  ## Remove generated schema files
	@echo "Cleaning generated schemas..."
	rm -rf generated_schemas/
	rm -f test/workflows/test_meta/schemas/dirac-metadata.json

test-schemas:  ## Test workflows that use generated schemas
	@echo "Testing workflows with generated schemas..."
	pytest test/test_workflows.py::test_run_job_success -k "test_meta" -v

lint:  ## Run linting and type checking
	mypy src

test:  ## Run all tests
	pytest test/ -v

test-meta:  ## Run only metadata-related tests
	pytest test/test_metadata*.py test/test_workflows.py::test_run_job_success -k "test_meta" -v

check-schemas:  ## Verify that schemas are up to date
	@echo "Checking if schemas are up to date..."
	@python scripts/generate_schemas.py --output-dir /tmp/check_schemas --format json --unified > /dev/null 2>&1
	@if ! diff -q generated_schemas/dirac-metadata.json /tmp/check_schemas/dirac-metadata.json > /dev/null 2>&1; then \
		echo "❌ Schemas are out of date. Run 'make schemas' to update them."; \
		exit 1; \
	else \
		echo "✅ Schemas are up to date."; \
	fi
	@rm -rf /tmp/check_schemas

validate-schemas:  ## Validate that generated schemas are syntactically correct
	@echo "Validating generated schemas..."
	@python -c "\
import json, yaml; \
from pathlib import Path; \
[print(f'✓ {f}') or json.load(open(f)) for f in Path('generated_schemas').glob('**/*.json')]; \
[print(f'✓ {f}') or yaml.safe_load(open(f)) for f in Path('generated_schemas').glob('**/*.yaml')]; \
print('All schemas are valid!')"

dev-setup: install schemas  ## Set up development environment
	@echo "Development environment set up successfully!"
	@echo "Generated schemas are in: generated_schemas/"
	@echo "Run 'make help' to see available commands."

# Example usage in CI/CD
ci-check: lint test check-schemas validate-schemas  ## Run all CI checks
