"""
I/O operations for Lance-Ray integration.
"""

from typing import Any, Literal, Optional

import pyarrow as pa
from ray.data import Dataset, read_datasource

from .datasink import LanceDatasink
from .datasource import LanceDatasource


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
    path: str,
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
        path: The path to the destination Lance dataset.
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
        path,
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
