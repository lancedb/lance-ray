
# Distributed Index Building

Lance-Ray provides distributed index building functionality that leverages Ray's distributed computing capabilities to efficiently create text indices for Lance datasets. This is particularly useful for large-scale datasets as it can distribute index building work across multiple Ray worker nodes.

## New Distributed APIs

`create_scalar_index()` - Distributedly create scalar index index using ray. Currently only Inverted/FTS are supported. Will add more index type support in the future.

### How It Works
The `create_scalar_index` function allows you to create full-text search indices for Lance datasets using the Ray distributed computing framework. This function distributes the index building process across multiple Ray worker nodes, with each node responsible for building indices for a subset of dataset fragments. These indices are then merged and committed as a single index.

**Backward Compatibility**:
   - Automatically detect availability of new APIs across different Lance versions
   - Gracefully fallback to raise tips when new APIs are unavailable


**`create_scalar_index`**

```python
def create_scalar_index(
    dataset: Union[str, "lance.LanceDataset"],
    column: str,
    index_type: str,
    name: Optional[str] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":

```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset` | `str` or `lance.LanceDataset` | Lance dataset or its URI |
| `column` | `str` | Column name to index |
| `index_type` | `str` | Index type, can be `"INVERTED"` or `"FTS"` |
| `name` | `str`, optional | Index name, auto-generated if not provided |
| `num_workers` | `int`, optional | Number of Ray worker nodes to use, default is 4 |
| `storage_options` | `Dict[str, str]`, optional | Storage options for the dataset |
| `ray_remote_args` | `Dict[str, Any]`, optional | Ray task options (e.g., `num_cpus`, `resources`) |
| `**kwargs` | `Any` | Additional arguments passed to `create_scalar_index` |

### Return Value

The function returns an updated Lance dataset with the newly created index.


## Examples

### Basic Usage

```python
import lance
import lance_ray as lr

# Create or load Lance dataset
dataset = lance.dataset("path/to/dataset")

# Build distributed index
updated_dataset = lr.create_scalar_index(
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
updated_dataset = lr.create_scalar_index(
   dataset="path/to/dataset",
   column="text",
   index_type="INVERTED",
   name="custom_text_index",
   num_workers=4
)
```

### Custom Ray Options

```python
updated_dataset = lr.create_scalar_index(
   dataset="path/to/dataset",
   column="text",
   index_type="INVERTED",
   num_workers=4,
   ray_remote_args={"num_cpus": 2, "resources": {"custom_resource": 1}}
)
```

### Performance Considerations

- For very large datasets, it's recommended to use more powerful CPU/memory ray worker nodes. Increasing `num_workers` can improve index building speed, but requires more computational nodes.
- Too many num_workers can cause large number of partitions, which cause FTS queries slowness as lots of index partitions need to be loaded when searching.
- If `num_workers` is greater than the number of fragments, it will be automatically adjusted to match the fragment count
