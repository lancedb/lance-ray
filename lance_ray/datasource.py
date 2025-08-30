import inspect
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional

import numpy as np
import pyarrow as pa
from ray.data._internal.util import _check_import, call_with_retry
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask

if TYPE_CHECKING:
    import lance
    from lance_namespace import LanceNamespace


class LanceDatasource(Datasource):
    """Lance datasource, for reading Lance dataset."""

    # Errors to retry when reading Lance fragments.
    READ_FRAGMENTS_ERRORS_TO_RETRY = ["LanceError(IO)"]
    # Maximum number of attempts to read Lance fragments.
    READ_FRAGMENTS_MAX_ATTEMPTS = 10
    # Maximum backoff seconds between attempts to read Lance fragments.
    READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 32

    def __init__(
        self,
        uri: Optional[str] = None,
        namespace: Optional["LanceNamespace"] = None,
        table_id: Optional[list[str]] = None,
        columns: Optional[list[str]] = None,
        filter: Optional[str] = None,
        storage_options: Optional[dict[str, str]] = None,
        scanner_options: Optional[dict[str, Any]] = None,
        dataset_options: Optional[dict[str, Any]] = None,
        fragment_ids: Optional[list[int]] = None,
    ):
        _check_import(self, module="lance", package="pylance")

        self._dataset_options = dataset_options or {}
        self._scanner_options = scanner_options or {}
        if columns is not None:
            self._scanner_options["columns"] = columns
        if filter is not None:
            self._scanner_options["filter"] = filter

        # Handle namespace-based table loading
        if namespace is not None and table_id is not None:
            # Import here to avoid circular dependency
            from lance_namespace import DescribeTableRequest

            # Get the table URI from the namespace
            describe_request = DescribeTableRequest(id=table_id)
            describe_response = namespace.describe_table(describe_request)
            self._uri = describe_response.location

            merged_storage_options = dict()
            if storage_options:
                merged_storage_options.update(storage_options)
            if describe_response.storage_options:
                merged_storage_options.update(describe_response.storage_options)
            self._storage_options = merged_storage_options
        else:
            self._uri = uri
            self._storage_options = storage_options

        match = []
        match.extend(self.READ_FRAGMENTS_ERRORS_TO_RETRY)
        match.extend(DataContext.get_current().retried_io_errors)
        self._retry_params = {
            "description": "read lance fragments",
            "match": match,
            "max_attempts": self.READ_FRAGMENTS_MAX_ATTEMPTS,
            "max_backoff_s": self.READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS,
        }
        self._fragment_ids = set(fragment_ids) if fragment_ids else None

        self._lance_ds = None
        self._fragments = None

    @property
    def lance_dataset(self) -> "lance.LanceDataset":
        if self._lance_ds is None:
            import lance

            dataset_options = self._dataset_options.copy()
            dataset_options["uri"] = self._uri
            dataset_options["storage_options"] = self._storage_options
            self._lance_ds = lance.dataset(**dataset_options)
        return self._lance_ds

    @property
    def fragments(self) -> list["lance.LanceFragment"]:
        if self._fragments is None:
            self._fragments = self.lance_dataset.get_fragments() or []
            if self._fragment_ids:
                self._fragments = [
                    f for f in self._fragments if f.metadata.id in self._fragment_ids
                ]
        return self._fragments

    def get_read_tasks(self, parallelism: int) -> list[ReadTask]:
        if not self.fragments:
            return []

        read_tasks = []

        for fragments in np.array_split(self.fragments, parallelism):
            if len(fragments) == 0:
                continue

            # Use scanner.count_rows with filter to count rows meeting specified conditions
            scanner_options = self._scanner_options.copy()
            scanner_options["fragments"] = fragments
            scanner_options["columns"] = []
            scanner_options["with_row_id"] = True
            scanner = self._lance_ds.scanner(**scanner_options)
            num_rows = scanner.count_rows()

            fragment_ids = [f.metadata.id for f in fragments]
            input_files = [
                data_file.path
                for fragment in fragments
                for data_file in fragment.data_files()
            ]

            # Ray 2.48+ no longer has the schema argument...
            if "schema" in inspect.signature(BlockMetadata.__init__).parameters:
                # TODO(chengsu): Take column projection into consideration for schema.
                metadata = BlockMetadata(
                    num_rows=num_rows,
                    schema=fragments[0].schema,
                    input_files=input_files,
                    size_bytes=None,
                    exec_stats=None,
                )
            else:
                metadata = BlockMetadata(
                    num_rows=num_rows,
                    input_files=input_files,
                    size_bytes=None,
                    exec_stats=None,
                )

            read_task = ReadTask(
                lambda fids=fragment_ids,
                lance_ds=self.lance_dataset,
                scanner_options=self._scanner_options,
                retry_params=self._retry_params: _read_fragments_with_retry(
                    fids, lance_ds, scanner_options, retry_params
                ),
                metadata,
            )

            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if not self.fragments:
            return 0

        return sum(
            data_file.file_size_bytes
            for fragment in self.fragments
            for data_file in fragment.data_files()
            if data_file.file_size_bytes is not None
        )


def _read_fragments_with_retry(
    fragment_ids: list[int],
    lance_ds: "lance.LanceDataset",
    scanner_options: dict[str, Any],
    retry_params: dict[str, Any],
) -> Iterator[pa.Table]:
    return call_with_retry(
        lambda: _read_fragments(fragment_ids, lance_ds, scanner_options),
        **retry_params,
    )


def _read_fragments(
    fragment_ids: list[int],
    lance_ds: "lance.LanceDataset",
    scanner_options: dict[str, Any],
) -> Iterator[pa.Table]:
    """Read Lance fragments in batches.

    NOTE: Use fragment ids, instead of fragments as parameter, because pickling
    LanceFragment is expensive.
    """
    fragments = [lance_ds.get_fragment(id) for id in fragment_ids]
    scanner_options["fragments"] = fragments
    scanner = lance_ds.scanner(**scanner_options)
    for batch in scanner.to_reader():
        yield pa.Table.from_batches([batch])
