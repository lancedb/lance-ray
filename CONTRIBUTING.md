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

## Release Process

We use an automated release process to manage version bumps and deployments to PyPI.

### Automatic Version Bumping

The project uses automatic version detection and bumping based on conventional commits:

- **`feat:`** commits trigger a minor version bump
- **`fix:`** commits trigger a patch version bump
- **`BREAKING CHANGE:`** or **`!:`** commits trigger a major version bump

To manually trigger a version bump:

1. Go to Actions → Auto Bump Version workflow
2. Click "Run workflow"
3. Select the bump type (auto/patch/minor/major)
4. The workflow will create a PR with the version changes

### Creating a Release

There are two release channels:

- **Stable**: Full releases published to PyPI
- **Preview**: Beta/preview releases for testing

To create a release:

1. **Ensure version is bumped** (if needed):
   - Either wait for automatic bump based on commits
   - Or manually trigger the Auto Bump Version workflow
   - Review and merge the generated PR

2. **Create the release**:
   - Go to Actions → Create Release workflow
   - Select release type (patch/minor/major)
   - Select release channel (stable/preview)
   - Set dry_run to false for actual release
   - Run the workflow

3. **Review and publish**:
   - The workflow creates a draft GitHub release
   - Review the auto-generated release notes
   - Edit if necessary
   - Publish the release

4. **Automatic PyPI deployment**:
   - Once the release is published, the Python Release workflow automatically triggers
   - The package is built and published to PyPI using trusted publishing
   - No manual intervention required

### Manual Release (Emergency)

If needed, you can manually trigger PyPI deployment:

1. Go to Actions → Python Release workflow
2. Select mode: "release"
3. Optionally specify a ref (branch/tag/SHA)
4. Run the workflow

### Version Management

- Version is managed in `pyproject.toml`
- The `.bumpversion.toml` file configures automatic version updates
- All version changes should go through the automated workflows

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

Examples:
- `feat: add support for Ray 2.5`
- `fix: resolve memory leak in datasink`
- `docs: update installation instructions`
- `chore: update dependencies`

### Release Checklist

- [ ] All tests passing
- [ ] Documentation updated
- [ ] CHANGELOG updated (automatic via release notes)
- [ ] Version bumped (automatic or manual)
- [ ] Release created and published
- [ ] PyPI deployment successful
- [ ] Announcement made (if major release)
