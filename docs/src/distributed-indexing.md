
# Distributed Index Building

Lance-Ray provides distributed index building functionality that leverages Ray's distributed computing capabilities to efficiently create text indices for Lance datasets. This is particularly useful for large-scale datasets as it can distribute index building work across multiple Ray worker nodes.

## New Features Highlights (Based on PR #4578)

The latest version of distributed indexing functionality is based on interface improvements from Lance PR #4578, providing more efficient and reliable distributed index building capabilities:

### Key Improvements

1. **New Distributed APIs**:
   - `create_fts_index()` - Distributedly create fts index index using ray

2. **Three-Phase Workflow**:
   - **Split and Parallel Phase**: Distribute dataset fragments to different Ray worker nodes
   - **Merge Phase**: Collect and merge index metadata from all worker nodes
   - **Commit Phase**: Atomically commit index information to the dataset

3. **Backward Compatibility**:
   - Automatically detect availability of new APIs
   - Gracefully fallback to traditional methods when new APIs are unavailable
   - Ensure proper functionality across different Lance versions

## Overview

The `create_fts_index` function allows you to create full-text search indices for Lance datasets using the Ray distributed computing framework. This function distributes the index building process across multiple Ray worker nodes, with each node responsible for building indices for a subset of dataset fragments. These indices are then merged and committed as a single index.

## Usage

```python
import lance_ray as lr

# Build distributed index
updated_dataset = lr.create_fts_index(
   dataset="path/to/dataset",  # or lance.LanceDataset object
   column="text_column",  # Text column name to index
   index_type="INVERTED",  # Index type, supports "INVERTED" or "FTS"
   name="my_text_index",  # Optional, index name
   num_workers=4,  # Optional, number of Ray worker nodes
)

# Use index for search
results = updated_dataset.scanner(
   full_text_query="search term",
   columns=["id", "text_column"]
).to_table()
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset` | `str` or `lance.LanceDataset` | Lance dataset or its URI |
| `column` | `str` | Column name to index |
| `index_type` | `str` | Index type, can be `"INVERTED"` or `"FTS"` |
| `name` | `str`, optional | Index name, auto-generated if not provided |
| `num_workers` | `int`, optional | Number of Ray worker nodes to use, default is 4 |
| `storage_options` | `Dict[str, str]`, optional | Storage options for the dataset |
| `ray_options` | `Dict[str, Any]`, optional | Ray task options (e.g., `num_cpus`, `resources`) |
| `**kwargs` | `Any` | Additional arguments passed to `create_scalar_index` |

## Return Value

The function returns an updated Lance dataset with the newly created index.

## Error Handling

The function will raise exceptions in the following situations:

- `ValueError`: If input parameters are invalid, such as empty column name, non-positive number of workers, invalid index type, etc.
- `TypeError`: If column type is not string
- `RuntimeError`: If index building fails

## How It Works

1. The function first validates input parameters and initializes Ray (if not already initialized)
2. It then distributes dataset fragments across multiple Ray worker nodes
3. Each worker node builds indices for its assigned fragments
4. Finally, all fragment indices are merged and committed as a single index to the dataset

## Performance Considerations

- Increasing `num_workers` can improve index building speed, but requires more computational resources
- For very large datasets, it's recommended to use more worker nodes
- If `num_workers` is greater than the number of fragments, it will be automatically adjusted to match the fragment count

## Examples

### Basic Usage

```python
import lance
import lance_ray as lr

# Create or load Lance dataset
dataset = lance.dataset("path/to/dataset")

# Build distributed index
updated_dataset = lr.create_fts_index(
   dataset=dataset,
   column="text",
   index_type="INVERTED",
   num_workers=4
)

# Verify index creation
indices = updated_dataset.list_indices()
print(f"Index list: {indices}")

# Use index for search
results = updated_dataset.scanner(
   full_text_query="search term",
   columns=["id", "text"]
).to_table()
print(f"Search results: {results}")
```

### Custom Index Name

```python
updated_dataset = lr.create_fts_index(
   dataset="path/to/dataset",
   column="text",
   index_type="INVERTED",
   name="custom_text_index",
   num_workers=4
)
```

### Using FTS Index Type

```python
updated_dataset = lr.create_fts_index(
   dataset="path/to/dataset",
   column="text",
   index_type="FTS",  # Use FTS index type
   num_workers=4
)
```

### Custom Ray Options

```python
updated_dataset = lr.create_fts_index(
   dataset="path/to/dataset",
   column="text",
   index_type="INVERTED",
   num_workers=4,
   ray_options={"num_cpus": 2, "resources": {"custom_resource": 1}}
)
```
