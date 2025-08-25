import pickle
from collections.abc import Iterable
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    Union,
)

import pyarrow as pa
from ray.data import DataContext
from ray.data._internal.util import _check_import, call_with_retry
from ray.data.block import BlockAccessor
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    import pandas as pd
    from lance.fragment import FragmentMetadata
    from lance_namespace import LanceNamespace


def _write_fragment(
    stream: Iterable[Union[pa.Table, "pd.DataFrame"]],
    uri: str,
    *,
    schema: Optional[pa.Schema] = None,
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    max_rows_per_group: int = 1024,  # Only useful for v1 writer.
    data_storage_version: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
    retry_params: Optional[dict[str, Any]] = None,
) -> list[tuple["FragmentMetadata", pa.Schema]]:
    import pandas as pd
    from lance.fragment import DEFAULT_MAX_BYTES_PER_FILE, write_fragments

    if schema is None:
        first = next(iter(stream))
        if isinstance(first, pd.DataFrame):
            schema = pa.Schema.from_pandas(first).remove_metadata()
        else:
            schema = first.schema
        if len(schema.names) == 0:
            # Empty table.
            schema = None

        stream = chain([first], stream)

    def record_batch_converter():
        for block in stream:
            tbl = BlockAccessor.for_block(block).to_arrow()
            yield from tbl.to_batches()

    max_bytes_per_file = (
        DEFAULT_MAX_BYTES_PER_FILE if max_bytes_per_file is None else max_bytes_per_file
    )

    reader = pa.RecordBatchReader.from_batches(schema, record_batch_converter())
    fragments = call_with_retry(
        lambda: write_fragments(
            reader,
            uri,
            schema=schema,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            max_bytes_per_file=max_bytes_per_file,
            data_storage_version=data_storage_version,
            storage_options=storage_options,
        ),
        **retry_params,
    )
    return [(fragment, schema) for fragment in fragments]


class _BaseLanceDatasink(Datasink):
    """Base class for Lance Datasink."""

    def __init__(
        self,
        uri: Optional[str] = None,
        namespace: Optional["LanceNamespace"] = None,
        table_id: Optional[list[str]] = None,
        *args: Any,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        storage_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        # Handle namespace-based table writing
        if namespace is not None and table_id is not None:
            self.namespace = namespace
            self.table_id = table_id

            if mode == "append":
                # For append mode, we need to get existing table URI
                from lance_namespace import DescribeTableRequest

                describe_request = DescribeTableRequest(id=table_id)
                describe_response = namespace.describe_table(describe_request)
                self.uri = describe_response.location
            else:
                # For create/overwrite modes, we'll determine URI based on namespace implementation
                # For now, we'll let the namespace handle the URI generation after write
                self.uri = None  # Will be set during write process
        else:
            self.namespace = None
            self.table_id = None
            self.uri = uri

        self.schema = schema
        self.mode = mode
        self.read_version: Optional[int] = None
        self.storage_options = storage_options

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self):
        _check_import(self, module="lance", package="pylance")

        import lance

        if self.mode == "append":
            ds = lance.LanceDataset(self.uri, storage_options=self.storage_options)
            self.read_version = ds.version
            if self.schema is None:
                self.schema = ds.schema

    def on_write_complete(
        self,
        write_result: list[list[tuple[str, str]]],
    ):
        import warnings

        import lance

        write_results = write_result
        if not write_results:
            warnings.warn(
                "write_results is empty.",
                DeprecationWarning,
                stacklevel=2,
            )
            return
        if hasattr(write_results, "write_returns"):
            write_results = write_results.write_returns  # type: ignore

        if len(write_results) == 0:
            warnings.warn(
                "write results is empty. please check ray version or internal error",
                DeprecationWarning,
                stacklevel=2,
            )
            return

        fragments = []
        schema = None
        for batch in write_results:
            for fragment_str, schema_str in batch:
                fragment = pickle.loads(fragment_str)
                fragments.append(fragment)
                schema = pickle.loads(schema_str)
        # Check weather writer has fragments or not.
        # Skip commit when there are no fragments.
        if not schema:
            return
        op = None
        if self.mode in {"create", "overwrite"}:
            op = lance.LanceOperation.Overwrite(schema, fragments)
        elif self.mode == "append":
            op = lance.LanceOperation.Append(fragments)
        if op:
            lance.LanceDataset.commit(
                self.uri,
                op,
                read_version=self.read_version,
                storage_options=self.storage_options,
            )

            # Register table with namespace if using namespace-based writing
            if (
                self.namespace is not None
                and self.table_id is not None
                and self.mode in {"create", "overwrite"}
            ):
                self._register_table_with_namespace()

    def _register_table_with_namespace(self):
        """Register the table with the namespace after successful write."""
        try:
            from lance_namespace import RegisterTableRequest

            # Map write mode to register mode
            register_mode = "CREATE" if self.mode == "create" else "OVERWRITE"

            register_request = RegisterTableRequest(
                id=self.table_id, location=self.uri, mode=register_mode
            )

            self.namespace.register_table(register_request)
        except Exception as e:
            import warnings

            warnings.warn(
                f"Failed to register table {self.table_id} with namespace: {e}",
                RuntimeWarning,
                stacklevel=3,
            )


class LanceDatasink(_BaseLanceDatasink):
    """Lance Ray Datasink.

    Write a Ray dataset to lance.

    If we expect to write larger-than-memory files,
    we can use `LanceFragmentWriter` and `LanceCommitter`.

    Args:
        uri : the base URI of the dataset.
        schema : pyarrow.Schema, optional.
            The schema of the dataset.
        mode : str, optional
            The write mode. Default is 'append'.
            Choices are 'append', 'create', 'overwrite'.
        min_rows_per_file : int, optional
            The minimum number of rows per file. Default is 1024 * 1024.
        max_rows_per_file : int, optional
            The maximum number of rows per file. Default is 64 * 1024 * 1024.
        data_storage_version: optional, str, default None
            The version of the data storage format to use. Newer versions are more
            efficient but require newer versions of lance to read.  The default is
            "legacy" which will use the legacy v1 version.  See the user guide
            for more details.
        storage_options : Dict[str, Any], optional
            The storage options for the writer. Default is None.
    """

    NAME = "Lance"
    WRITE_FRAGMENTS_ERRORS_TO_RETRY = ["LanceError(IO)"]
    WRITE_FRAGMENTS_MAX_ATTEMPTS = 10
    WRITE_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 32

    def __init__(
        self,
        uri: Optional[str] = None,
        namespace: Optional["LanceNamespace"] = None,
        table_id: Optional[list[str]] = None,
        *args: Any,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        min_rows_per_file: int = 1024 * 1024,
        max_rows_per_file: int = 64 * 1024 * 1024,
        data_storage_version: Optional[str] = None,
        storage_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(
            uri,
            namespace,
            table_id,
            *args,
            schema=schema,
            mode=mode,
            storage_options=storage_options,
            **kwargs,
        )

        self.min_rows_per_file = min_rows_per_file
        self.max_rows_per_file = max_rows_per_file
        self.data_storage_version = data_storage_version
        # if mode is append, read_version is read from existing dataset.
        self.read_version: Optional[int] = None

        match = []
        match.extend(self.WRITE_FRAGMENTS_ERRORS_TO_RETRY)
        match.extend(DataContext.get_current().retried_io_errors)
        self._retry_params = {
            "description": "write lance fragments",
            "match": match,
            "max_attempts": self.WRITE_FRAGMENTS_MAX_ATTEMPTS,
            "max_backoff_s": self.WRITE_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS,
        }

    @property
    def min_rows_per_write(self) -> int:
        return self.min_rows_per_file

    def get_name(self) -> str:
        return self.NAME

    def write(
        self,
        blocks: Iterable[Union[pa.Table, "pd.DataFrame"]],
        ctx: Any,
    ):
        fragments_and_schema = _write_fragment(
            blocks,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
            retry_params=self._retry_params,
        )
        return [
            (pickle.dumps(fragment), pickle.dumps(schema))
            for fragment, schema in fragments_and_schema
        ]
