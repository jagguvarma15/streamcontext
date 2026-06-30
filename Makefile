# Common developer tasks. Requires uv (https://github.com/astral-sh/uv).
.PHONY: help setup lint type test test-integration check run-gateway run-mcp run-catalog compose-up compose-down

help:
	@echo "setup             Create the venv and install dev dependencies"
	@echo "lint              Run ruff"
	@echo "type              Run mypy"
	@echo "test              Run the unit test suite"
	@echo "test-integration  Run the integration suite (needs Docker)"
	@echo "check             Run lint, type, and unit tests"
	@echo "run-gateway       Start the ingestion gateway"
	@echo "run-mcp           Start the MCP server (stdio)"
	@echo "run-catalog       Run one catalog refresh pass"
	@echo "compose-up        Start Kafka + Schema Registry + Qdrant + gateway"
	@echo "compose-down      Stop the local stack"

setup:
	uv sync --extra dev

lint:
	uv run ruff check .

type:
	uv run mypy

test:
	uv run pytest -q

test-integration:
	RUN_INTEGRATION=1 uv run pytest -q

check: lint type test

run-gateway:
	uv run streamcontext

run-mcp:
	uv run streamcontext-mcp

run-catalog:
	uv run python -m streamcontext.catalog.refresher

compose-up:
	docker compose up -d

compose-down:
	docker compose down
