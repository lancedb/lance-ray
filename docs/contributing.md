# Contributing to lance-ray

## Development setup

Install the latest development version with all dependencies:

```bash
git clone https://github.com/<your-username>/lance-ray.git
cd lance-ray
uv pip install -e .[dev]
```
# Requirements

- Python >= 3.10

- Ray >= 2.40.0

- PyLance >= 0.30.0

- lance-namespace >= 0.0.5

- PyArrow >= 17.0.0

- NumPy >= 2.0.0

## Optional Dependencies
- pandas >= 2.2.0
Required only if you want to use DataFrame inputs.
Install with:
```bash
pip install lance-ray[pandas]
```
# Running Tests

To run all tests using [pytest](https://docs.pytest.org/):

```bash
uv run pytest
```
