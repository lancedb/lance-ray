"""Test cases for lance_ray.fragment module."""

from pathlib import Path

import lance
import pandas as pd
import pyarrow as pa
import pytest
import ray

from lance_ray.fragment import (
    LanceCommitter,
    LanceFragmentWriter,
    _register_hooks,
    add_columns as add_columns_distributed,
)


@pytest.fixture(scope="module", autouse=True)
def ray_context():
    """Initialize Ray for testing."""
    if ray.is_initialized():
        ray.shutdown()
    ray.init(ignore_reinit_error=True)
    yield
    if ray.is_initialized():
        ray.shutdown()


class TestLanceFragmentWriterCommitter:
    """Test cases for LanceFragmentWriter and LanceCommitter."""

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_committer(self, tmp_path: Path):
        """Test fragment writer and committer for large-scale data."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Use fragment writer and committer
        (
            ray.data.range(10)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema), batch_size=5)
            .write_datasink(LanceCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        assert ds.schema == schema

        tbl = ds.to_table()
        assert sorted(tbl["id"].to_pylist()) == list(range(10))
        assert set(tbl["str"].to_pylist()) == set([f"str-{i}" for i in range(10)])
        # Should have 2 fragments since batch_size=5 and we have 10 rows
        assert len(ds.get_fragments()) == 2

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_with_transform(self, tmp_path: Path):
        """Test fragment writer with custom transform function."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("str", pa.string()),
                pa.field("doubled", pa.int64()),
            ]
        )

        def transform(batch: pa.Table) -> pa.Table:
            """Transform function to add a doubled column."""
            df = batch.to_pandas()
            df["doubled"] = df["id"] * 2
            return pa.Table.from_pandas(df, schema=schema)

        # Use fragment writer with transform
        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(
                LanceFragmentWriter(tmp_path, schema=schema, transform=transform),
                batch_size=5,
            )
            .write_datasink(LanceCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 5
        tbl = ds.to_table()
        assert tbl.column("doubled").to_pylist() == [0, 2, 4, 6, 8]

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_append_mode(self, tmp_path: Path):
        """Test fragment writer with append mode."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Write initial data
        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceCommitter(tmp_path, mode="create"))
        )

        # Append more data
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] >= 5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceCommitter(tmp_path, mode="append"))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        assert sorted(tbl["id"].to_pylist()) == list(range(10))

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_empty_write(self, tmp_path: Path):
        """Test fragment writer with empty data."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Write empty data (filter everything out)
        (
            ray.data.range(10)
            .filter(lambda row: row["id"] > 10)  # Filter out everything
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceCommitter(tmp_path))
        )

        # Empty write should not create a dataset
        with pytest.raises(ValueError):
            lance.dataset(tmp_path)

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_fragment_writer_none_values(self, tmp_path: Path):
        """Test fragment writer with None values."""

        def create_row(row):
            return {
                "id": row["id"],
                "str": None if row["id"] % 2 == 0 else f"str-{row['id']}",
            }

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        (
            ray.data.range(10)
            .map(create_row)
            .map_batches(LanceFragmentWriter(tmp_path, schema=schema))
            .write_datasink(LanceCommitter(tmp_path))
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        tbl = ds.to_table()
        str_values = tbl["str"].to_pylist()
        id_values = tbl["id"].to_pylist()
        # Even IDs should have None values
        for id_val, str_val in zip(id_values, str_values):
            if id_val % 2 == 0:
                # None values might be represented as None or as 'nan' string
                assert str_val is None or str(str_val) == 'nan', f"ID {id_val} should have None/nan but got {str_val}"
            else:
                assert str_val == f"str-{id_val}", f"ID {id_val} should have 'str-{id_val}' but got {str_val}"


class TestRayHooks:
    """Test Ray hook registration for write_lance."""

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_register_hooks_write_lance(self, tmp_path: Path):
        """Test _register_hooks enables ray.data.Dataset.write_lance."""
        # Register hooks
        _register_hooks()

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

        # Now we should be able to use write_lance directly on Ray dataset
        (
            ray.data.range(10)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .write_lance(tmp_path, schema=schema)
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 10
        assert ds.schema == schema
        tbl = ds.to_table()
        assert sorted(tbl["id"].to_pylist()) == list(range(10))
        assert set(tbl["str"].to_pylist()) == set([f"str-{i}" for i in range(10)])

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_hooks_write_lance_with_transform(self, tmp_path: Path):
        """Test write_lance with transform after registering hooks."""
        _register_hooks()

        def transform(batch: pa.Table) -> pa.Table:
            """Add a new column with doubled values."""
            df = batch.to_pandas()
            df["doubled"] = df["id"] * 2
            return pa.Table.from_pandas(df)

        (
            ray.data.range(5)
            .map(lambda x: {"id": x["id"], "str": f"str-{x['id']}"})
            .write_lance(tmp_path, transform=transform)
        )

        # Verify the dataset
        ds = lance.dataset(tmp_path)
        assert ds.count_rows() == 5
        tbl = ds.to_table()
        assert "doubled" in tbl.column_names
        assert tbl.column("doubled").to_pylist() == [0, 2, 4, 6, 8]


class TestDistributedAddColumns:
    """Test distributed add_columns using fragment API."""

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_distributed_add_columns(self, tmp_path: Path):
        """Test distributed column addition through fragment API."""

        def generate_label(batch: pa.RecordBatch) -> pa.RecordBatch:
            heights = batch.column("height").to_pylist()
            tags = ["big" if height > 5 else "small" for height in heights]
            df = pd.DataFrame({"size_labels": tags})

            return pa.RecordBatch.from_pandas(
                df, schema=pa.schema([pa.field("size_labels", pa.string())])
            )

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("height", pa.int64()),
                pa.field("weight", pa.int64()),
            ]
        )

        # Register hooks and write initial data
        _register_hooks()
        (
            ray.data.range(11)
            .repartition(1)
            .map(lambda x: {"id": x["id"], "height": (x["id"] + 5), "weight": x["id"]})
            .write_lance(tmp_path, schema=schema)
        )

        # Add columns using distributed fragment API
        lance_ds = lance.dataset(tmp_path)
        add_columns_distributed(lance_ds, generate_label, source_columns=["height"])

        # Verify the new column was added
        ds = lance.dataset(tmp_path)
        tbl = ds.to_table()
        assert "size_labels" in tbl.column_names
        size_labels = tbl.column("size_labels").to_pylist()
        heights = tbl.column("height").to_pylist()
        # Check that labels match the height criteria
        for height, label in zip(heights, size_labels):
            if height > 5:
                assert label == "big", f"Height {height} should be 'big' but got '{label}'"
            else:
                assert label == "small", f"Height {height} should be 'small' but got '{label}'"

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_distributed_add_columns_multiple_fragments(self, tmp_path: Path):
        """Test distributed add_columns with multiple fragments."""

        def add_squared(batch: pa.RecordBatch) -> pa.RecordBatch:
            values = batch.column("value").to_pylist()
            squared = [v * v for v in values]
            return pa.RecordBatch.from_pandas(
                pd.DataFrame({"squared": squared}),
                schema=pa.schema([pa.field("squared", pa.int64())]),
            )

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])

        # Write data with multiple fragments
        _register_hooks()
        (
            ray.data.range(20)
            .map(lambda x: {"id": x["id"], "value": x["id"] + 1})
            .write_lance(tmp_path, schema=schema, max_rows_per_file=5)
        )

        # Verify we have multiple fragments
        lance_ds = lance.dataset(tmp_path)
        assert len(lance_ds.get_fragments()) == 4  # 20 rows / 5 rows per file

        # Add columns using distributed API
        add_columns_distributed(lance_ds, add_squared, source_columns=["value"])

        # Verify the new column was added correctly
        ds = lance.dataset(tmp_path)
        tbl = ds.to_table()
        assert "squared" in tbl.column_names
        values = tbl.column("value").to_pylist()
        squared = tbl.column("squared").to_pylist()
        for v, s in zip(values, squared):
            assert s == v * v