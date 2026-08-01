.PHONY: help lint typecheck test

.DEFAULT_GOAL := help

help:
	@echo "Available commands:"
	@echo "  make lint      - Run ruff and pylint"
	@echo "  make typecheck - Run mypy static type checking"
	@echo "  make test      - Run tests"

lint:
	poetry run ruff check src
	poetry run pylint src

typecheck:
	poetry run mypy src

test:
	poetry run pytest -v
