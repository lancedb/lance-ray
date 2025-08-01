
.PHONY: build
build:
	uv pip install ".[dev]"
	uv sync --dev

.PHONY: test
test:
	uv run pytest -vv -s