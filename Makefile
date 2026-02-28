.PHONY: install setup lint format typecheck test test-cov infra-up infra-down seed build publish-test publish clean all

install:
	uv sync --all-extras

setup:
	@test -f .env || cp dev/.env.example .env
	@echo "Created .env -- iceberg-meta loads it automatically"

lint:
	uv run ruff check src/ dev/tests/

format:
	uv run ruff format src/ dev/tests/

typecheck:
	uv run mypy src/

test:
	uv run pytest dev/tests/ -v --tb=short

test-cov:
	uv run pytest dev/tests/ -v --tb=short --cov=iceberg_meta --cov-report=term-missing

infra-up:
	docker compose --env-file .env -f dev/docker-compose.yml up -d

infra-down:
	docker compose -f dev/docker-compose.yml down -v

seed:
	uv run python dev/scripts/seed_local_catalog.py

build:
	uv build

publish-test:
	uv publish --repository testpypi

publish:
	uv publish

clean:
	rm -rf dist/ build/ .venv/ .mypy_cache/ .pytest_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name '*.egg-info' -exec rm -rf {} + 2>/dev/null || true

all: lint format typecheck test
