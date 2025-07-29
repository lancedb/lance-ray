# Lance-Ray Integration

A Python library that provides seamless integration between [Ray](https://www.ray.io/) and [Lance](https://lancedb.github.io/lance/) for distributed columnar data processing.

## Overview

Lance-Ray combines the distributed computing capabilities of Ray with the efficient columnar storage format of Lance, enabling scalable data processing workflows with optimal performance.

## Features

- **Distributed Lance Operations:** Leverage Ray’s distributed computing for Lance dataset operations.
- **Seamless Data Movement:** Efficiently move data between Ray and Lance datasets.
- **Optimized I/O:** Fast read and write operations on Lance datasets with Ray integration.
- **Parallel Processing:** Support for concurrent batch operations on distributed Lance data.

## Quick Start

```python

import ray

from lance_ray import read_lance, write_lance

ray.init()

# Write a pandas DataFrame to Lance format
import pandas as pd

df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

write_lance("example.lance", df)

# Read the dataset back as a Ray Dataset
ds = read_lance("example.lance")

print(ds.take(3))
``` 
