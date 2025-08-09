# Contributing to lance-ray

## Development setup

Install the latest development version with all dependencies:

```bash
git clone https://github.com/<your-username>/lance-ray.git
cd lance-ray
uv pip install -e .[dev]
```
# Requirements

- Python >= 3.8

- Ray >= 2.40.0

- PyLance >= 0.30.0

- lance-namespace >= 0.0.5

- PyArrow >= 17.0.0

- Pandas >= 2.2.0

- NumPy >= 2.0.0


# Running Tests

To run all tests using [pytest](https://docs.pytest.org/):

```bash
uv run pytest
```
