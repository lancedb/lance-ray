# Contributing to Lance-Ray

Thank you for your interest in contributing to Lance-Ray! 
This document provides guidelines and instructions for contributing to the project.

## Development Setup

### Prerequisites

- Python >= 3.10
- UV package manager
- Git

### Setting Up Your Development Environment

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
```

## Development Workflow

### Running Tests

Run the test suite to ensure everything is working:

```bash
# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov=lance_ray

# Run specific test file
uv run pytest tests/test_basic_read_write.py

# Run tests in parallel
uv run pytest -n auto
```

### Code Quality

We use `ruff` for both linting and formatting:

```bash
# Format code
uv run ruff format lance_ray/ tests/ examples/

# Check linting
uv run ruff check lance_ray/ tests/ examples/

# Fix linting issues automatically
uv run ruff check --fix lance_ray/ tests/ examples/
```

### Pre-commit Checks

Before committing your changes, ensure:

1. All tests pass
2. Code is properly formatted
3. Linting checks pass
4. Any new functionality has tests

### 4. Update Documentation

If your changes affect the public API:
- Update relevant documentation in `docs/src/`
- Update docstrings
- Add examples if appropriate

### 5. Commit Your Changes

Write clear, descriptive commit messages:

```bash
git add .
git commit -m "feat: add support for custom storage options

- Add storage_options parameter to read_lance and write_lance
- Support S3, Azure, and GCS backends
- Add tests for cloud storage integration"
```

Follow conventional commit format:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions/changes
- `refactor:` for code refactoring
- `chore:` for maintenance tasks

## Submitting a Pull Request

1. **Push your branch**

```bash
git push origin feature/your-feature-name
```

2. **Create a Pull Request**

- Go to the [Lance-Ray repository](https://github.com/lancedb/lance-ray)
- Click "New Pull Request"
- Select your branch
- Fill out the PR template with:
  - Clear description of changes
  - Related issue numbers (if any)
  - Test plan
  - Screenshots (if UI changes)

3. **PR Guidelines**

- Keep PRs focused on a single feature/fix
- Ensure CI checks pass
- Respond to review feedback promptly
- Update your branch with main if needed:

```bash
git fetch upstream
git rebase upstream/main
```

## Code Style Guidelines

### Python Style

- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Keep functions focused and small
- Avoid deep nesting

### Docstring Format

Use Google-style docstrings:

```python
def read_lance(uri: str, columns: List[str] = None) -> ray.data.Dataset:
    """Read a Lance dataset and return a Ray Dataset.
    
    Args:
        uri: The URI of the Lance dataset to read.
        columns: Optional list of column names to read.
        
    Returns:
        A Ray Dataset containing the Lance data.
        
    Raises:
        FileNotFoundError: If the dataset doesn't exist.
    """
```

### Type Hints

Use type hints for function signatures:

```python
from typing import Optional, List, Dict, Any

def process_data(
    data: ray.data.Dataset,
    options: Optional[Dict[str, Any]] = None
) -> ray.data.Dataset:
    ...
```

## Testing Guidelines

### Test Organization

- Place tests in `tests/` directory
- Mirror the source code structure
- Use descriptive test file names

### Test Coverage

- Aim for high test coverage (>80%)
- Test edge cases and error conditions
- Use fixtures for common test data

### Running Specific Tests

```bash
# Run tests matching a pattern
uv run pytest -k "test_read"

# Run with verbose output
uv run pytest -v

# Run with debugging
uv run pytest --pdb
```

## Debugging

### Using Ray Dashboard

When debugging Ray-related issues:

```python
import ray
ray.init(dashboard_host="0.0.0.0")
# Access dashboard at http://localhost:8265
```

### Logging

Add logging for debugging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logger.debug("Processing batch with %d rows", len(batch))
```

## Getting Help

If you need help:

1. Check the [documentation](https://lancedb.github.io/lance-ray/)
2. Search existing [issues](https://github.com/lancedb/lance-ray/issues)
3. Join our [Discord community](https://discord.gg/zMM32dvNtd)
4. Create a new issue with:
   - Clear problem description
   - Minimal reproducible example
   - Environment details (Python version, OS, etc.)

## License

By contributing to Lance-Ray, you agree that your contributions will be licensed under the Apache License 2.0.

## Thank You!

Your contributions make Lance-Ray better for everyone. We appreciate your time and effort!