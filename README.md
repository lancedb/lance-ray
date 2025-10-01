# Lance-Ray

[![PyPI](https://img.shields.io/pypi/v/lance-ray.svg)](https://pypi.org/project/lance-ray/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://lancedb.github.io/lance/integrations/ray)

A Python library that provides seamless integration between 
[Ray](https://ray.io/) and [Lance](https://lancedb.github.io/lance/) for distributed data processing.


Lance-Ray combines the distributed computing capabilities of Ray with the efficient columnar storage format of Lance, enabling scalable data processing workflows with optimal performance.

## Features

- **Distributed Lance Operations**: Leverage Ray's distributed computing for Lance dataset operations
- **Seamless Data Conversion**: Easy conversion between Ray datasets and Lance datasets
- **Optimized I/O**: Efficient reading and writing of Lance datasets with Ray integration
- **Schema Validation**: Automatic schema compatibility checking between Ray and Lance
- **Flexible Filtering**: Support for complex filtering operations on distributed Lance data
- **Catalog Integration**: Support for working with Lance datasets stored in various catalog services (e.g. Hive MetaStore, Iceberg REST Catalog, Unity, Gravitino, AWS Glue, etc.)

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

- Python >= 3.10
- Ray >= 2.40.0
- PyLance >= 0.30.0
- lance-namespace >=0.0.5
- PyArrow >= 17.0.0
- NumPy >= 2.0.0

### Optional Dependencies

By default, `pandas` is not installed with Lance-Ray.
If you want to use features that accept pandas DataFrames as input, install with:

```bash
pip install lance-ray[pandas]
```

## Quick Start

```python
import ray
from lance_ray import read_lance, write_lance

# Initialize Ray
ray.init()

# Create a Ray dataset
data = ray.data.range(1000).map(lambda row: {"id": row["id"], "value": row["id"] * 2})

# Write to Lance format
write_lance(data, "my_dataset.lance")

# Read Lance dataset back as Ray dataset
ray_dataset = read_lance("my_dataset.lance")

# Perform distributed operations
result = ray_dataset.filter(lambda row: row["value"] > 100).count()
print(f"Filtered count: {result}")
```

### Using Lance Namespace

For enterprise environments with metadata catalogs, you can use Lance Namespace integration:

```python
import ray
import lance_namespace as ln
from lance_ray import read_lance, write_lance

# Initialize Ray
ray.init()

# Connect to a metadata catalog (directory-based example)
namespace = ln.connect("dir", {"root": "/path/to/tables"})

# Create a Ray dataset
data = ray.data.range(1000).map(lambda row: {"id": row["id"], "value": row["id"] * 2})

# Write to Lance format using metadata catalog
write_lance(data, namespace=namespace, table_id=["my_table"])

# Read Lance dataset back using metadata catalog
ray_dataset = read_lance(namespace=namespace, table_id=["my_table"])

# Perform distributed operations
result = ray_dataset.filter(lambda row: row["value"] > 100).count()
print(f"Filtered count: {result}")
```

## API Reference

### I/O Functions

#### `read_lance(uri=None, *, namespace=None, table_id=None, columns=None, filter=None, storage_options=None, **kwargs)`

Read a Lance dataset and return a Ray Dataset.

**Parameters:**
- `uri`: The URI of the Lance dataset to read from (either uri OR namespace+table_id required)
- `namespace`: LanceNamespace instance for metadata catalog integration (requires table_id)
- `table_id`: Table identifier as list of strings (requires namespace)
- `columns`: Optional list of column names to read
- `filter`: Optional filter expression to apply
- `storage_options`: Optional storage configuration dictionary
- `scanner_options`: Optional scanner configuration dictionary
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks
- `override_num_blocks`: Optional override for number of output blocks

**Returns:** Ray Dataset

#### `write_lance(ds, uri=None, *, namespace=None, table_id=None, schema=None, mode="create", **kwargs)`

Write a Ray Dataset to Lance format.

**Parameters:**
- `ds`: Ray Dataset to write
- `uri`: Path to the destination Lance dataset (either uri OR namespace+table_id required)
- `namespace`: LanceNamespace instance for metadata catalog integration (requires table_id)
- `table_id`: Table identifier as list of strings (requires namespace)
- `schema`: Optional PyArrow schema
- `mode`: Write mode - "create", "append", or "overwrite"
- `min_rows_per_file`: Minimum rows per file (default: 1024 * 1024)
- `max_rows_per_file`: Maximum rows per file (default: 64 * 1024 * 1024)
- `data_storage_version`: Optional data storage version
- `storage_options`: Optional storage configuration dictionary
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `concurrency`: Optional maximum number of concurrent Ray tasks

**Returns:** None

#### `add_columns(uri=None, *, namespace=None, table_id=None, transform, **kwargs)`

Add columns to an existing Lance dataset using Ray's distributed processing.

**Parameters:**
- `uri`: Path to the Lance dataset (either uri OR namespace+table_id required)
- `namespace`: LanceNamespace instance for metadata catalog integration (requires table_id)
- `table_id`: Table identifier as list of strings (requires namespace)
- `transform`: Transform function to apply for adding columns
- `filter`: Optional filter expression to apply
- `read_columns`: Optional list of columns to read from original dataset
- `reader_schema`: Optional schema for the reader
- `read_version`: Optional version to read
- `ray_remote_args`: Optional kwargs for Ray remote tasks
- `storage_options`: Optional storage configuration dictionary
- `batch_size`: Batch size for processing (default: 1024)
- `concurrency`: Optional number of concurrent processes

**Returns:** None

## Examples

### Basic Usage

**Note:** The following example requires the optional `pandas` dependency.
Install with:
```bash
pip install lance-ray[pandas]
```

```python
import pandas as pd
import ray
from lance_ray import read_lance, write_lance

# Initialize Ray
ray.init()

# Create sample data
sample_data = {
    "user_id": range(100),
    "name": [f"User_{i}" for i in range(100)],
    "age": [20 + (i % 50) for i in range(100)],
    "score": [50.0 + (i % 100) * 0.5 for i in range(100)],
}
df = pd.DataFrame(sample_data)

# Create Ray dataset
ds = ray.data.from_pandas(df)

# Write to Lance format
write_lance(ds, "sample_dataset.lance")

# Read Lance dataset back
ds = read_lance("sample_dataset.lance")

# Perform distributed operations
filtered_ds = ds.filter(lambda row: row["age"] > 30)
print(f"Filtered count: {filtered_ds.count()}")

# Read with column selection and filtering
ds_filtered = read_lance(
    "sample_dataset.lance",
    columns=["user_id", "name", "score"],
    filter="score > 75.0"
)
print(f"Schema: {ds_filtered.schema()}")
```

### Advanced Usage

```python
# Write with custom options
write_lance(
    ds,
    "dataset.lance",
    mode="overwrite",
    min_rows_per_file=1000,
    max_rows_per_file=50000,
    data_storage_version="stable"
)

# Read with storage options and concurrency control
ds = read_lance(
    "s3://bucket/dataset.lance",
    storage_options={"aws_access_key_id": "...", "aws_secret_access_key": "..."},
    concurrency=10,
    ray_remote_args={"num_cpus": 2}
)

# Using different metadata catalog backends
import lance_namespace as ln

# Directory-based namespace (for local development)
dir_namespace = ln.connect("dir", {"root": "/local/tables"})

# REST API-based namespace (for enterprise catalogs like Unity, Gravitino)
rest_namespace = ln.connect("rest", {
    "base_url": "https://catalog-api.example.com",
    "api_key": "your-api-key"
})

# Write using metadata catalog
write_lance(ds, namespace=dir_namespace, table_id=["processed_data"], mode="overwrite")

# Read using metadata catalog
ds = read_lance(namespace=dir_namespace, table_id=["processed_data"])

# Add columns using metadata catalog
from lance_ray import add_columns
import pyarrow as pa

def add_computed_column(batch: pa.RecordBatch) -> pa.RecordBatch:
    df = batch.to_pandas()
    df['computed'] = df['value'] * 2 + df['id']
    return pa.RecordBatch.from_pandas(df[["computed"]])

add_columns(
    namespace=dir_namespace,
    table_id=["processed_data"],
    transform=add_computed_column,
    concurrency=4
)
```

See the `examples/` directory for more comprehensive usage examples:

- `basic_usage.py`: Basic Ray-Lance integration workflow

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/lance-ray/lance-ray.git
cd lance-ray

# Install in development mode
uv pip install -e ".[dev]"

```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=lance_ray


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
- [User Guide and API Documentation](https://lancedb.github.io/lance/integrations/ray/)  
- [Contributing Guide and Dev Setup](./CONTRIBUTING.md)
 
