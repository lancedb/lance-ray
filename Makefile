.PHONY: lint
lint: lock
	uv run ruff check
	uv run ruff format --check .

.PHONY: fix
fix: lock
	uv run ruff check --fix --unsafe-fixes
	uv run ruff format .

.PHONY: lock
lock:
	uv lock --extra-index-url https://pypi.org/simple --extra-index-url https://pypi.fury.io/lancedb --index-strategy unsafe-best-match

.PHONY: build
build: lock
	uv pip install ".[dev]"

.PHONY: test
test: lock
	uv run pytest -vv -s