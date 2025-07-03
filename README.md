# Lance-Ray Integration

A Python library that provides seamless integration between [Ray](https://ray.io/) and [Lance](https://lancedb.github.io/lance/) for distributed columnar data processing.

## Overview

Lance-Ray combines the distributed computing capabilities of Ray with the efficient columnar storage format of Lance, enabling scalable data processing workflows with optimal performance.

## Features

- **Distributed Lance Operations**: Leverage Ray's distributed computing for Lance dataset operations
- **Seamless Data Conversion**: Easy conversion between Ray datasets and Lance datasets
- **Optimized I/O**: Efficient reading and writing of Lance datasets with Ray integration
- **Schema Validation**: Automatic schema compatibility checking between Ray and Lance
- **Flexible Filtering**: Support for complex filtering operations on distributed Lance data

## Installation

```bash
# Install from source
git clone https://github.com/lance-ray/lance-ray.git
cd lance-ray
uv pip install -e .

# Or install with development dependencies
uv pip install -e ".[dev]"
```

## Requirements

- Python >= 3.8
- Ray > 2.41
- PyLance (latest version)
- PyArrow >= 12.0.0
- Pandas >= 1.5.0
- NumPy >= 1.21.0

## Quick Start

```python
import ray
from lance_ray import read_lance, write_lance, LanceRayDataset

# Initialize Ray
ray.init()

# Create a Ray dataset
data = ray.data.range(1000).map(lambda x: {"id": x, "value": x * 2})

# Write to Lance format
write_lance(data, "my_dataset.lance")

# Read Lance dataset
lance_dataset = read_lance("my_dataset.lance")

# Convert to Ray dataset for distributed processing
ray_dataset = lance_dataset.to_ray_dataset()

# Perform distributed operations
result = ray_dataset.filter(lambda row: row["value"] > 100).count()
print(f"Filtered count: {result}")
```

## API Reference

### Core Classes

#### `LanceRayDataset`

Main class for Ray-integrated Lance datasets.

```python
dataset = LanceRayDataset("path/to/dataset")

# Convert to Ray dataset
ray_data = dataset.to_ray_dataset()

# Read as pandas DataFrame
df = dataset.read_pandas()

# Get schema information
schema = dataset.schema()
row_count = dataset.count_rows()

# Filter and select operations
filtered = dataset.filter("age > 30")
selected = dataset.select(["id", "name"])
```

### I/O Functions

#### `read_lance(uri, columns=None, filter=None, **kwargs)`

Read a Lance dataset with optional column selection and filtering.

#### `write_lance(data, uri, mode="create", **kwargs)`

Write Ray dataset, Arrow table, or pandas DataFrame to Lance format.

- `mode`: "create", "append", or "overwrite"

#### `read_lance_distributed(uri, num_partitions=None, **kwargs)`

Read Lance dataset in a distributed manner across Ray workers.

### Utility Functions

#### `create_lance_from_ray(ray_dataset, uri, **kwargs)`

Create a Lance dataset from a Ray dataset.

#### `get_dataset_info(dataset)`

Get comprehensive information about a Lance dataset.

#### `validate_schema_compatibility(ray_schema, lance_schema)`

Validate schema compatibility between Ray and Lance datasets.

## Examples

See the `examples/` directory for more comprehensive usage examples:

- `basic_usage.py`: Basic Ray-Lance integration workflow
- `distributed_processing.py`: Advanced distributed processing patterns
- `performance_optimization.py`: Performance optimization techniques

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/lance-ray/lance-ray.git
cd lance-ray

# Install in development mode
uv pip install -e ".[dev]"

# Install pre-commit hooks
uv run pre-commit install
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=lance_ray

# Run specific test categories
uv run pytest -m unit          # Unit tests only
uv run pytest -m integration   # Integration tests only
uv run pytest -m slow          # Slow tests only
```

### Code Quality

```bash
# Format code
uv run ruff format lance_ray/ tests/ examples/

# Lint code
uv run ruff check lance_ray/ tests/ examples/

```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature-name`)
3. Make your changes
4. Add tests for new functionality
5. Run the test suite and ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Ray](https://ray.io/) for distributed computing framework
- [Lance](https://lancedb.github.io/lance/) for columnar storage format
- [Apache Arrow](https://arrow.apache.org/) for in-memory data structures
