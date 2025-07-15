"""
I/O operations for Lance-Ray integration.
"""

import pickle
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, Optional

import pyarrow as pa
from lance.dataset import LanceDataset, LanceOperation
from lance.udf import BatchUDF
from ray.data import Dataset, read_datasource
from ray.util.multiprocessing import Pool

from .datasink import LanceDatasink
from .datasource import LanceDatasource
from .scanner import RayScannerBuilder

if TYPE_CHECKING:
    from lance.types import ReaderLike

    TransformType = (
        dict[str, str]
        | BatchUDF
        | ReaderLike
        | Callable[[pa.RecordBatch], pa.RecordBatch]
    )


def read_lance(
    uri: str,
    *,
    columns: Optional[list[str]] = None,
    filter: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    scanner_options: Optional[dict[str, Any]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """
    Create a :class:`~ray.data.Dataset` from a
    `Lance Dataset <https://lancedb.github.io/lance/api/py_modules.html#lance.dataset.LanceDataset>`_.

    Examples:
        >>> import lance_ray as lr
        >>> ds = lr.read_lance( # doctest: +SKIP
        ...     uri="./db_name.lance",
        ...     columns=["image", "label"],
        ...     filter="label = 2 AND text IS NOT NULL",
        ... )

    Args:
        uri: The URI of the Lance dataset to read from. Local file paths, S3, and GCS
            are supported.
        columns: The columns to read. By default, all columns are read.
        filter: Read returns only the rows matching the filter. By default, no
            filter is applied.
        storage_options: Extra options that make sense for a particular storage
            connection. This is used to store connection parameters like credentials,
            endpoint, etc. For more information, see `Object Store Configuration <https\
                ://lancedb.github.io/lance/object_store.html#object-store-configuration>`_.
        scanner_options: Additional options to configure the `LanceDataset.scanner()`
            method, such as `batch_size`. For more information,
            see `LanceDB API doc <https://lancedb.github.io/\
                lance/api/py_modules.html#lance.LanceDataset.scanner>`_
        ray_remote_args: kwargs passed to :func:`ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`~ray.data.Dataset` producing records read from the Lance dataset.
    """  # noqa: E501
    datasource = LanceDatasource(
        uri=uri,
        columns=columns,
        filter=filter,
        storage_options=storage_options,
        scanner_options=scanner_options,
    )

    return read_datasource(
        datasource=datasource,
        ray_remote_args=ray_remote_args or {},
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


def write_lance(
    ds: Dataset,
    uri: str,
    *,
    schema: Optional[pa.Schema] = None,
    mode: Literal["create", "append", "overwrite"] = "create",
    min_rows_per_file: int = 1024 * 1024,
    max_rows_per_file: int = 64 * 1024 * 1024,
    data_storage_version: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    """Write the dataset to a Lance dataset.

    Examples:
            .. testcode::
            import lance_ray as lr
            import pandas as pd

            docs = [{"title": "Lance data sink test"} for key in range(4)]
            ds = ray.data.from_pandas(pd.DataFrame(docs))
            lr.write_lance(ds, "/tmp/data/")

    Args:
        uri: The path to the destination Lance dataset.
        schema: The schema of the dataset. If not provided, it is inferred from the data.
        mode: The write mode. Can be "create", "append", or "overwrite".
        min_rows_per_file: The minimum number of rows per file.
        max_rows_per_file: The maximum number of rows per file.
        data_storage_version: The version of the data storage format to use. Newer versions are more
            efficient but require newer versions of lance to read.  The default is
            "legacy" which will use the legacy v1 version.  See the user guide
            for more details.
        storage_options: The storage options for the writer. Default is None.
    """
    datasink = LanceDatasink(
        uri,
        schema=schema,
        mode=mode,
        min_rows_per_file=min_rows_per_file,
        max_rows_per_file=max_rows_per_file,
        data_storage_version=data_storage_version,
        storage_options=storage_options,
    )

    ds.write_datasink(
        datasink,
        ray_remote_args=ray_remote_args or {},
        concurrency=concurrency,
    )


def _handle_fragment(
    lance_ds: LanceDataset,
    transform: "TransformType",
    read_columns: Optional[list[str]] = None,
    batch_size: Optional[int] = None,
    reader_schema: Optional[pa.Schema] = None,
):
    """
    Handle a fragment of a Lance dataset.
    """

    def func(fragment_id: int):
        fragment = lance_ds.get_fragment(fragment_id)
        fragment_meta, schema = fragment.merge_columns(
            transform, read_columns, batch_size, reader_schema
        )
        return pickle.dumps(fragment_meta), pickle.dumps(schema)

    return func


def add_columns(
    uri: str,
    *,
    transform: "TransformType",
    filter: Optional[str] = None,
    read_columns: Optional[list[str]] = None,
    reader_schema: Optional[pa.Schema] = None,
    read_version: Optional[int | str] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    storage_options: Optional[dict[str, Any]] = None,
    batch_size: int = 1024,
    concurrency: Optional[int] = None,
) -> None:
    """
    Add columns to a Lance dataset, currently use ray.util.multiprocessing.Pool to implement it. ray.data API is hard to implement.
    Example:
        >>> import lance_ray as lr
        >>> import pyarrow as pa
        >>> import pandas as pd
        >>> ds = ray.data.from_pandas(pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}))
        >>> lr.write_lance(ds, "/tmp/data/")
        >>> def double_score(x: pa.RecordBatch) -> pa.RecordBatch:
        ...     df = x.to_pandas()
        ...     return pa.RecordBatch.from_pandas(
        ...         pd.DataFrame({"new_column": df["score"] * 2}),
        ...         schema=pa.schema([pa.field("new_column", pa.float64())]),
        ...     )
        >>> lr.add_columns("/tmp/data/", transform=double_score, concurrency=2)
    Args:
        uri: The path to the destination Lance dataset.
        transform: The transform to apply to the dataset. It support a lot of types, see `LanceDB API doc https://lancedb.github.io/lance-python-doc/data-evolution.html ` for more details.
        filter: The filter to apply to the dataset. It is not supported yet, will be supported when `get_fragments` support filter see `LanceDB API doc <https://lancedb.github.io/lance-python-doc/all-modules.html#lance.LanceDataset.get_fragments>`_.
        read_columns: The columns from the original dataset to read.
        reader_schema: The schema to use for the reader.
        read_version: The version to read.
        ray_remote_args: The arguments to pass to the ray remote function.
        storage_options: The storage options to use for the dataset.
        batch_size: The batch size to use for the reader.
        concurrency: The number of processes to use for the pool.
    """
    lance_ds = LanceDataset(
        uri=uri, storage_options=storage_options, version=read_version
    )
    fragment_ids = [f.metadata.id for f in lance_ds.get_fragments()]
    pool = Pool(processes=concurrency, ray_remote_args=ray_remote_args)
    rst_futures = pool.map_async(
        _handle_fragment(lance_ds, transform, read_columns, batch_size, reader_schema),
        fragment_ids,
        chunksize=1,
    )
    result = rst_futures.get()
    commit_messages = []
    new_schema = None
    for fragment_meta, schema in result:
        commit_messages.append(pickle.loads(fragment_meta))
        schema = pickle.loads(schema)
        if new_schema is None:
            new_schema = schema
            continue
        if new_schema != schema:
            raise ValueError(
                f"Schema mismatch, previous schema: {new_schema}, new schema: {schema}"
            )
    if new_schema is None:
        raise ValueError("No schema for new fragment found")
    op = LanceOperation.Merge(commit_messages, new_schema)
    lance_ds.commit(
        uri,
        op,
        read_version=lance_ds.version,
        storage_options=storage_options,
    )
    pool.close()


def scanner(
    uri: str,
    *,
    storage_options: Optional[dict[str, Any]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> RayScannerBuilder:
    return RayScannerBuilder(
        uri,
        storage_options=storage_options,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )
