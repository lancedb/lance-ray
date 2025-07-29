# Contributing

We welcome contributions from the community!

## How to Contribute

1. **Fork** the repository.
2. **Clone** your fork locally.
3. **Create a new branch** for your feature or bugfix.
4. **Make your changes** and add tests if applicable.
5. **Commit** and **push** your changes.
6. **Open a pull request** to the main repository.

## Guidelines

- Ensure your code passes all tests before submitting a pull request.
- Write clear and descriptive commit messages.
- Document any new features or changes in the relevant `.md` files.
- Follow the existing code style and conventions.

## Development setup

Install the latest development version with all dependencies:

```bash
git clone https://github.com/<your-username>/lance-ray.git
cd lance-ray
pip install -e .[dev]
```
# Requirements

- Python >= 3.8

- Ray >= 2.0.0

- Lance >= 0.2.0

# Running Tests

To run all tests using [pytest](https://docs.pytest.org/):

```bash
pytest
```
