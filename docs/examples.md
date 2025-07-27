# Examples

## Basic Usage

```python

import ray

from lance_ray import read_lance, write_lance

ray.init()

# Write a DataFrame to Lance
import pandas as pd

df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

write_lance("example.lance", df)

# Read the dataset back
ds = read_lance("example.lance")

print(ds.take(3))

# Read only specific columns
ds = read_lance("example.lance", columns=["a"])

print(ds.take(3))

# Read with a filter expression
ds = read_lance("example.lance", filters="a > 1")

print(ds.take(3))

# Process data in parallel using Ray tasks
@ray.remote

def process_partition(partition):
    return [x * 2 for x in partition["a"]]

partitions = ds.split(2)

results = ray.get([process_partition.remote(p) for p in partitions])

print(results)