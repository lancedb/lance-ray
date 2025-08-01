"""Test cases for lance_ray.io module."""

import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pandas as pd
import pyarrow as pa
import pytest
import ray
from ray.data import Dataset


@pytest.fixture(scope="session", autouse=True)
def ray_context():
    """Initialize Ray for testing."""
    # Shutdown Ray if it's already running to avoid conflicts
    if ray.is_initialized():
        ray.shutdown()

    # Initialize Ray with minimal configuration
    ray.init(local_mode=False, ignore_reinit_error=True)
    yield

    # Clean shutdown
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [85.5, 92.0, 78.5, 88.0, 95.5],
        }
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_dataset(sample_data):
    """Create a Ray Dataset from sample data."""
    return ray.data.from_pandas(sample_data)


class TestWriteLance:
    """Test cases for write_lance function."""

    def test_write_lance_basic(self, sample_dataset, temp_dir):
        """Test basic write functionality."""
        path = Path(temp_dir) / "basic_write.lance"

        lr.write_lance(sample_dataset, str(path))

        assert path.exists()
        assert path.is_dir()

    def test_write_lance_with_schema(self, temp_dir):
        """Test write with explicit schema."""
        path = Path(temp_dir) / "schema_write.lance"

        data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        dataset = ray.data.from_pandas(data)

        schema = pa.schema(
            [pa.field("col1", pa.int64()), pa.field("col2", pa.string())]
        )

        lr.write_lance(dataset, str(path), schema=schema)
        assert path.exists()

    def test_write_lance_invalid_input(self, temp_dir):
        """Test error handling for invalid inputs."""
        path = Path(temp_dir) / "invalid.lance"

        with pytest.raises((ValueError, AttributeError, TypeError)):
            lr.write_lance(None, str(path))  # type: ignore


class TestReadLance:
    """Test cases for read_lance function."""

    @pytest.fixture
    def lance_dataset_path(self, sample_dataset, temp_dir):
        """Create a Lance dataset for reading tests."""
        path = Path(temp_dir) / "test_dataset.lance"
        lr.write_lance(sample_dataset, str(path))
        return str(path)

    def test_read_lance_basic(self, lance_dataset_path):
        """Test basic read functionality."""
        dataset = lr.read_lance(lance_dataset_path)

        assert isinstance(dataset, Dataset)

        df = dataset.to_pandas()
        assert len(df) == 5
        assert list(df.columns) == ["id", "name", "age", "score"]

    def test_read_lance_with_columns(self, lance_dataset_path):
        """Test reading specific columns."""
        dataset = lr.read_lance(lance_dataset_path, columns=["id", "name"])

        df = dataset.to_pandas()
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 5

    def test_read_lance_with_filter(self, lance_dataset_path):
        """Test reading with filter."""
        dataset = lr.read_lance(lance_dataset_path, filter="age > 30")

        df = dataset.to_pandas()
        assert len(df) == 3
        assert all(df["age"] > 30)

    def test_read_lance_columns_and_filter(self, lance_dataset_path):
        """Test reading with both columns and filter."""
        dataset = lr.read_lance(
            lance_dataset_path, columns=["name", "age"], filter="age >= 35"
        )

        df = dataset.to_pandas()
        assert list(df.columns) == ["name", "age"]
        assert len(df) == 3
        assert all(df["age"] >= 35)

    def test_read_lance_filter_and_count(self, lance_dataset_path):
        """Test reading filter and count."""
        dataset = lr.read_lance(
            lance_dataset_path, columns=["name", "age"], filter="age >= 35"
        )
        assert dataset.count() == 3

    def test_read_lance_nonexistent_path(self):
        """Test reading from non-existent path."""
        with pytest.raises((FileNotFoundError, OSError, Exception)):
            lr.read_lance("/path/that/does/not/exist")


class TestReadWrite:
    """Integration tests for read and write operations."""

    def test_write_then_read_roundtrip(self, sample_data, temp_dir):
        """Test writing data and then reading it back."""
        path = Path(temp_dir) / "roundtrip.lance"

        # Write original data
        original_dataset = ray.data.from_pandas(sample_data)
        lr.write_lance(original_dataset, str(path))

        # Read it back
        read_dataset = lr.read_lance(str(path))
        read_df = read_dataset.to_pandas()

        # Compare data (sort by id to ensure consistent order)
        original_sorted = sample_data.sort_values("id").reset_index(drop=True)
        read_sorted = read_df.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(original_sorted, read_sorted)

    def test_append_mode(self, sample_data, temp_dir):
        """Test append mode with read verification."""
        path = Path(temp_dir) / "append_test.lance"

        # Write initial data
        initial_dataset = ray.data.from_pandas(sample_data[:3])
        lr.write_lance(initial_dataset, str(path))

        # Append more data
        additional_data = pd.DataFrame(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "age": [50, 55],
                "score": [90.0, 85.0],
            }
        )
        additional_dataset = ray.data.from_pandas(additional_data)
        lr.write_lance(additional_dataset, str(path), mode="append")

        # Read all data
        full_dataset = lr.read_lance(str(path))
        full_df = full_dataset.to_pandas()

        assert len(full_df) == 5  # 3 initial + 2 appended

    def test_overwrite_mode(self, sample_dataset, temp_dir):
        """Test different write modes."""
        path = Path(temp_dir) / "modes_test.lance"

        # Test create mode
        lr.write_lance(sample_dataset, str(path), mode="create")
        assert path.exists()

        # Verify initial row count
        initial_dataset = lr.read_lance(str(path))
        initial_df = initial_dataset.to_pandas()
        assert len(initial_df) == 5

        # Create dataset with 2 additional rows
        additional_data = pd.DataFrame(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "age": [50, 55],
                "score": [90.0, 85.0],
            }
        )
        extended_dataset = ray.data.from_pandas(additional_data)

        # Test overwrite mode with extended dataset
        lr.write_lance(extended_dataset, str(path), mode="overwrite")
        assert path.exists()

        # Verify row count after overwrite
        overwritten_dataset = lr.read_lance(str(path))
        overwritten_df = overwritten_dataset.to_pandas()
        assert len(overwritten_df) == 2  # Should have 2 rows after overwrite


class TestAddColumns:
    """Test cases for add_columns function."""

    def test_add_columns_basic(self, sample_dataset, temp_dir):
        """Test basic add columns functionality."""
        path = Path(temp_dir) / "add_columns_test.lance"
        lr.write_lance(sample_dataset, str(path), max_rows_per_file=3)

        def double_score(x: pa.RecordBatch) -> pa.RecordBatch:
            df = x.to_pandas()
            return pa.RecordBatch.from_pandas(
                pd.DataFrame({"new_column": df["score"] * 2}),
                schema=pa.schema([pa.field("new_column", pa.float64())]),
            )

        # Add columns
        lr.add_columns(
            str(path),
            transform=double_score,
            concurrency=2,
        )

        # Read it back
        dataset = lr.read_lance(str(path))
        df = dataset.to_pandas()
        assert df.columns.tolist() == ["id", "name", "age", "score", "new_column"]
        assert (df["new_column"] == df["score"] * 2).all()


class TestDatasetOptions:
    """Test cases for dataset options in LanceDataset."""

    def test_dataset_with_version(self, sample_dataset, temp_dir):
        """Test dataset options like version and block size."""
        path = Path(temp_dir) / "dataset_options_test.lance"
        lr.write_lance(sample_dataset, str(path))
        lr.write_lance(sample_dataset, str(path), mode="append")

        ds = lance.dataset(str(path))
        versions = ds.versions()
        assert len(versions) == 2
        assert len(ds) == 10

        dataset = lr.read_lance(
            str(path),
            dataset_options={
                "version": versions[0]["version"],
            },
        )
        assert dataset.count() == 5
        dataset = lr.read_lance(
            str(path),
            dataset_options={
                "version": versions[1]["version"],
            },
        )
        assert dataset.count() == 10
