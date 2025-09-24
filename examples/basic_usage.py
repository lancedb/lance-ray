"""
Basic usage example for Lance-Ray integration.

This example demonstrates how to:
1. Create a Lance dataset from Ray data
2. Read Lance data back into Ray
"""

import shutil
import tempfile
from pathlib import Path

import ray
from lance_ray import read_lance, write_lance

import pandas as pd


def main():
    """Main example function."""
    # Initialize Ray
    ray.init(ignore_reinit_error=True)

    print("Lance-Ray Integration Example")
    print("=" * 40)

    try:
        # Create sample data
        print("\n1. Creating sample data...")
        sample_data = {
            "user_id": range(100),
            "name": [f"User_{i}" for i in range(100)],
            "age": [20 + (i % 50) for i in range(100)],
            "score": [50.0 + (i % 100) * 0.5 for i in range(100)],
        }
        df = pd.DataFrame(sample_data)
        print(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")

        # Create Ray dataset
        print("\n2. Converting to Ray dataset...")
        ds = ray.data.from_pandas(df)
        print(f"Ray dataset created with {ds.count()} rows")

        # Create temporary directory for Lance dataset
        temp_dir = tempfile.mkdtemp()
        lance_path = Path(temp_dir) / "sample_dataset"

        # Write to Lance format
        print(f"\n3. Writing Ray dataset to Lance format at {lance_path}...")
        write_lance(ds, str(lance_path))
        print("Lance dataset created successfully")

        # Read Lance dataset back
        print("\n4. Reading Lance dataset...")
        ds = read_lance(str(lance_path))
        print(f"Schema: {ds.schema()}")
        print(f"Row count: {ds.count()}")
    except Exception as e:
        print(f"Error: {e}")
        raise

    finally:
        # Cleanup
        print("\n6. Cleaning up...")
        ray.shutdown()
        if "temp_dir" in locals():
            shutil.rmtree(temp_dir)
        print("Cleanup completed")

    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()
