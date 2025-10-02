import pickle
from collections.abc import Iterable
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

import pyarrow as pa
from ray.data._internal.block_accessor import BlockAccessor
from ray.data._internal.util import _check_import
from ray.data.datasource.datasink import Datasink

from .fragment import write_fragment

if TYPE_CHECKING:
    from lance.fragment import FragmentMetadata
    from lance_namespace import LanceNamespace

    import pandas as pd


def _write_fragment(
    stream: Iterable[Union[pa.Table, "pd.DataFrame"]],
    uri: str,
    *,
    schema: Optional[pa.Schema] = None,
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    max_rows_per_group: int = 1024,
    data_storage_version: Optional[str] = None,
    storage_options: Optional[dict[str, Any]] = None,
) -> list[tuple["FragmentMetadata", pa.Schema]]:
    try:
        import pandas as pd
    except ImportError:
        pd = None

    from lance.fragment import DEFAULT_MAX_BYTES_PER_FILE, write_fragments

    if schema is None:
        first = next(iter(stream))

        if isinstance(first, dict) and pd is None:
            raise ImportError(
                "pandas is required to handle DataFrame inputs. "
                "Install with `pip install lance-ray[pandas]`"
            )
        if pd is not None and isinstance(first, pd.DataFrame):
            schema = pa.Schema.from_pandas(first).remove_metadata()
        else:
            schema = first.schema
        if len(schema.names) == 0:
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
    fragments = write_fragments(
        reader,
        uri,
        schema=schema,
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=max_rows_per_group,
        max_bytes_per_file=max_bytes_per_file,
        data_storage_version=data_storage_version,
        storage_options=storage_options,
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

        merged_storage_options = dict()
        if storage_options:
            merged_storage_options.update(storage_options)

        if namespace is not None and table_id is not None:
            self.table_id = table_id

            if mode == "append":
                from lance_namespace import DescribeTableRequest

                describe_request = DescribeTableRequest(id=table_id)
                describe_response = namespace.describe_table(describe_request)
                self.uri = describe_response.location
                if describe_response.storage_options:
                    merged_storage_options.update(describe_response.storage_options)
            elif mode == "overwrite":
                from lance_namespace import DescribeTableRequest

                try:
                    describe_request = DescribeTableRequest(id=table_id)
                    describe_response = namespace.describe_table(describe_request)
                    self.uri = describe_response.location
                    if describe_response.storage_options:
                        merged_storage_options.update(describe_response.storage_options)
                except Exception:
                    self.uri = None
            else:
                self.uri = None
        else:
            self.table_id = None
            self.uri = uri

        self.schema = schema
        self.mode = mode
        self.read_version: Optional[int] = None
        self.storage_options = merged_storage_options

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

    def _register_table_with_namespace(self):
        try:
            from lance_namespace import RegisterTableRequest

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
    NAME = "Lance"

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
        self.read_version: Optional[int] = None

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
        fragments_and_schema = write_fragment(
            blocks,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
        )
        return [
            (pickle.dumps(fragment), pickle.dumps(schema))
            for fragment, schema in fragments_and_schema
        ]
