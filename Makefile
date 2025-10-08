.PHONY: build deploy run-dev

build:
	python3 tools/build.py

deploy: build
	databricks bundle deploy --target dev

run-dev: build
	databricks bundle run fr24_bronze_to_silver --target dev -- --job-parameters env=dev,catalog=test,run_date_utc=2025-10-06

