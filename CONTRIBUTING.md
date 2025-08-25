# Contributing to Lance-Ray

Thank you for your interest in contributing to Lance-Ray! 
This document provides guidelines and instructions for contributing to the project.

## Prerequisites

- Python >= 3.10
- UV package manager
- Git

## Setting Up Your Development Environment

1. **Fork and clone the repository**

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/lance-ray.git
cd lance-ray
```

2. **Install UV** (if not already installed)

```bash
pip install uv
```

3. **Install the project in development mode**

```bash
# Install with all development dependencies
uv pip install -e ".[dev]"

# To work on documentation, also install docs dependencies
uv pip install -e ".[dev,docs]"
```

## Running Tests

Run the test suite to ensure everything is working:

```bash
# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov=lance_ray

# Run specific test file
uv run pytest tests/test_basic_read_write.py -vv
```

## Check Styles

We use `ruff` for both linting and formatting:

```bash
# Format code
uv run ruff format lance_ray/ tests/ examples/

# Check linting
uv run ruff check lance_ray/ tests/ examples/

# Fix linting issues automatically
uv run ruff check --fix lance_ray/ tests/ examples/
```

## Building Documentation Locally

```bash
# Serve documentation locally
cd docs
uv run mkdocs serve

# Documentation will be available at http://localhost:8000
```
