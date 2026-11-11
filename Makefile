.PHONY: help lint typecheck test docs docs-serve

.DEFAULT_GOAL := help

POETRY := $(shell command -v poetry 2>/dev/null)

ifeq ($(POETRY),)
RUN := python3 -m
else
RUN := poetry run
endif

help:
	@echo "Available commands:"
	@echo "  make lint       - Run ruff and pylint"
	@echo "  make typecheck  - Run mypy static type checking"
	@echo "  make test       - Run tests"
	@echo "  make docs       - Build documentation site"
	@echo "  make docs-serve - Serve documentation locally"

lint:
	$(RUN) ruff check src
	$(RUN) pylint src

typecheck:
	$(RUN) mypy src

test:
	$(RUN) pytest -v

docs:
	$(RUN) zensical build

docs-serve:
	$(RUN) zensical serve
