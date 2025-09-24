# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import pickle
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Optional,
    Union,
)

import pyarrow as pa

import lance

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "LanceFragmentWriter",
    "LanceFragmentCommitter",
]

from .datasink import _write_fragment, _BaseLanceDatasink


class LanceFragmentWriter:
    """Write a fragment to one of Lance fragment.

    This Writer can be used in case to write large-than-memory data to lance,
    in distributed fashion.

    Parameters
    ----------
    uri : str
        The base URI of the dataset.
    transform : Callable[[pa.Table], Union[pa.Table, Generator]], optional
        A callable to transform the input batch. Default is None.
    schema : pyarrow.Schema, optional
        The schema of the dataset.
    max_rows_per_file : int, optional
        The maximum number of rows per file. Default is 1024 * 1024.
    max_bytes_per_file : int, optional
        The maximum number of bytes per file. Default is 90GB.
    max_rows_per_group : int, optional
        The maximum number of rows per group. Default is 1024.
        Only useful for v1 writer.
    data_storage_version: optional, str, default None
        The version of the data storage format to use. Newer versions are more
        efficient but require newer versions of lance to read.  The default
        (None) will use the 2.0 version.  See the user guide for more details.
    use_legacy_format : optional, bool, default None
        Deprecated method for setting the data storage version. Use the
        `data_storage_version` parameter instead.
    storage_options : Dict[str, Any], optional
        The storage options for the writer. Default is None.

    """

    def __init__(
        self,
        uri: str,
        *,
        transform: Optional[Callable[[pa.Table], Union[pa.Table, Generator]]] = None,
        schema: Optional[pa.Schema] = None,
        max_rows_per_file: int = 1024 * 1024,
        max_bytes_per_file: Optional[int] = None,
        max_rows_per_group: Optional[int] = None,  # Only useful for v1 writer.
        data_storage_version: Optional[str] = None,
        use_legacy_format: Optional[bool] = False,
        storage_options: Optional[Dict[str, Any]] = None,
    ):
        if use_legacy_format is not None:
            warnings.warn(
                "The `use_legacy_format` parameter is deprecated. Use the "
                "`data_storage_version` parameter instead.",
                DeprecationWarning,
            )

            if use_legacy_format:
                data_storage_version = "legacy"
            else:
                data_storage_version = "stable"

        self.uri = uri
        self.schema = schema
        self.transform = transform if transform is not None else lambda x: x

        self.max_rows_per_group = max_rows_per_group
        self.max_rows_per_file = max_rows_per_file
        self.max_bytes_per_file = max_bytes_per_file
        self.data_storage_version = data_storage_version
        self.storage_options = storage_options

    def __call__(self, batch: Union[pa.Table, "pd.DataFrame", Dict]) -> pa.Table:
        """Write a Batch to the Lance fragment."""
        # Convert dict/numpy arrays to pyarrow table if needed
        if isinstance(batch, dict):
            batch = pa.Table.from_pydict(batch)
        elif hasattr(batch, '__dataframe__'):  # pandas DataFrame
            batch = pa.Table.from_pandas(batch)

        transformed = self.transform(batch)
        if not isinstance(transformed, Generator):
            transformed = (t for t in [transformed])

        fragments = _write_fragment(
            transformed,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            max_rows_per_group=self.max_rows_per_group,
            max_bytes_per_file=self.max_bytes_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
            # retry_params defaults to None, which will use minimal retry settings
        )
        return pa.Table.from_pydict(
            {
                "fragment": [pickle.dumps(fragment) for fragment, _ in fragments],
                "schema": [pickle.dumps(schema) for _, schema in fragments],
            }
        )


class LanceFragmentCommitter(_BaseLanceDatasink):
    """Lance Committer as Ray Datasink.

    This is used with `LanceFragmentWriter` to write large-than-memory data to
    lance file.
    """

    @property
    def num_rows_per_write(self) -> int:
        return 1

    def get_name(self) -> str:
        return f"LanceCommitter({self.mode})"

    def write(
        self,
        blocks: Iterable[Union[pa.Table, "pd.DataFrame"]],
        _ctx,
    ):
        """Passthrough the fragments to commit phase"""
        v = []
        for block in blocks:
            # If block is empty, skip to get "fragment" and "schema" filed
            if len(block) == 0:
                continue

            for fragment, schema in zip(
                block["fragment"].to_pylist(), block["schema"].to_pylist()
            ):
                v.append((fragment, schema))
        return v


