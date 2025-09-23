# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import pickle
import warnings
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa
import ray
import ray.data
from ray.data import from_items

import lance

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "LanceFragmentWriter",
    "LanceCommitter",
    "execute_fragment_operation",
    "DispatchFragmentTasks",
    "FragmentTask",
    "AddColumnTask",
]

# ==============================================================================
# Constants
# ==============================================================================
NONE_ARROW_STR = "None"

# Component Keys
ITEM_KEY = "item"
READ_COLUMNS_KEY = "read_columns"
ACTION_KEY = "action"
ADD_COLUMN_ACTION = "add_column"

# Message Structure Constants
TASK_ID_KEY = "task_id"
PARTITION_KEY = "partition"

# Data Component Keys
FRAGMENT_KEY = "fragment"
SCHEMA_KEY = "schema"

# Operation Parameters
PARAMS_KEY = "params"

# Execution Metadata
OPERATION_TYPE_KEY = "operation_type"
VERSION_KEY = "version"

# ==============================================================================
# Type Aliases
# ==============================================================================
RecordBatchTransformer = Callable[[pa.RecordBatch], pa.RecordBatch]


# ==============================================================================
# Imports from other modules
# ==============================================================================
from .datasink import _write_fragment
from .utils import _pd_to_arrow


# ==============================================================================
# Distributed Task Classes
# ==============================================================================
@dataclass
class TaskInput:
    """Container for task execution parameters and metadata."""

    task_id: str
    fn: Callable
    fragment: Any
    params: Dict[str, Any] = field(default_factory=dict)


class FragmentTask:
    """Base class for distributed data processing tasks."""

    def __init__(self, task_input: TaskInput):
        self.task_input = task_input

    def __call__(self) -> Dict[str, Any]:
        output = self._fn()
        return {
            TASK_ID_KEY: self.task_input.task_id,
            PARTITION_KEY: {FRAGMENT_KEY: self.task_input.fragment, "output": output},
        }


class AddColumnTask(FragmentTask):
    """Task for adding new columns to dataset fragments."""

    def __init__(self, task_input: TaskInput, read_columns):
        super().__init__(task_input)
        self._read_columns = read_columns
        self._validate_input_params()

    def _validate_input_params(self) -> None:
        """Ensure required parameters are present and valid."""
        if self.task_input.fragment is None:
            raise ValueError("Fragment must be provided for column addition")

    def __call__(self) -> Dict[str, Any]:
        """Execute column addition and return updated fragment metadata."""
        new_fragment, new_schema = self.task_input.fragment.merge_columns(
            value_func=self.task_input.fn, columns=self._read_columns
        )
        return {
            TASK_ID_KEY: self.task_input.task_id,
            PARTITION_KEY: {FRAGMENT_KEY: new_fragment, SCHEMA_KEY: new_schema},
        }


class DispatchFragmentTasks:
    """Orchestrates distributed execution of fragment operations."""

    def __init__(self, dataset: lance.LanceDataset):
        self.dataset = dataset

    def get_tasks(
        self, transform_fn: Callable, operation_params: Optional[Dict[str, Any]] = None
    ) -> List[FragmentTask]:
        """Generate tasks for processing all dataset fragments."""
        operation_params = operation_params or {}
        return [
            self._create_task(fragment, transform_fn, operation_params)
            for fragment in self.dataset.get_fragments()
        ]

    def _create_task(
        self, fragment: Any, transform_fn: Callable, params: Dict[str, Any]
    ) -> FragmentTask:
        """Factory method for creating appropriate task type."""
        task_input = TaskInput(
            task_id=fragment.fragment_id,
            fn=transform_fn,
            fragment=fragment,
            params=params,
        )

        if params[ACTION_KEY] == "add_column":
            return AddColumnTask(task_input, params[READ_COLUMNS_KEY])

        raise ValueError(f"Unsupported operation: {params[ACTION_KEY]}")

    def commit_results(self, partitions: List[Dict[str, Any]]) -> bool:
        """Commit processed results to the dataset."""
        if not partitions:
            return False

        fragments = [part[FRAGMENT_KEY] for part in partitions]
        unified_schema = partitions[0][SCHEMA_KEY]

        operation = lance.LanceOperation.Merge(fragments, unified_schema)
        lance.LanceDataset.commit(
            self.dataset.uri,
            operation,
            read_version=self.dataset.version,
        )
        return True


# ==============================================================================
# Fragment API Functions
# ==============================================================================
def execute_fragment_operation(
    task_dispatcher: "DispatchFragmentTasks",
    value_function: Union[Dict[str, str], RecordBatchTransformer],
    operation_parameters: Dict[str, Any] = None,
) -> None:
    """
    Execute distributed fragment operations and commit results.

    Args:
        task_dispatcher: Coordinator for fragment tasks
        value_function: Data transformation logic
        operation_parameters: Contextual parameters for the operation
    """
    operation_parameters = operation_parameters or {}

    # Generate and execute distributed tasks
    processing_tasks = task_dispatcher.get_tasks(value_function, operation_parameters)
    # Create items dict for ray dataset
    task_items = [{ITEM_KEY: task} for task in processing_tasks]
    task_dataset = from_items(task_items).map(lambda row: row[ITEM_KEY]())

    # Collect and commit results
    results = [item[PARTITION_KEY] for item in task_dataset.take_all()]
    task_dispatcher.commit_results(results)




# ==============================================================================
# Base Lance Datasink
# ==============================================================================
class _BaseLanceDatasink(ray.data.Datasink):
    """Base Lance Ray Datasink."""

    def __init__(
        self,
        uri: str,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        storage_options: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.uri = uri
        self.schema = schema
        self.mode = mode

        self.read_version: int | None = None
        self.storage_options = storage_options

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self):
        if self.mode == "append":
            ds = lance.LanceDataset(self.uri, storage_options=self.storage_options)
            self.read_version = ds.version
            if self.schema is None:
                self.schema = ds.schema

    def on_write_complete(
        self,
        write_results: List[List[Tuple[str, str]]],
    ):
        if not write_results:
            warnings.warn(
                "write_results is empty.",
                DeprecationWarning,
            )
            return
        if (
            not isinstance(write_results, list)
            or not isinstance(write_results[0], list)
        ) and not hasattr(write_results, "write_returns"):
            warnings.warn(
                "write_results type is wrong. please check version, "
                "upgrade or downgrade your ray version. ray versions >= 2.38 "
                "and < 2.41 are unable to write Lance datasets, check ray PR "
                "https://github.com/ray-project/ray/pull/49251 in your "
                "ray version. ",
                DeprecationWarning,
            )
            return
        if hasattr(write_results, "write_returns"):
            write_results = write_results.write_returns

        if len(write_results) == 0:
            warnings.warn(
                "write results is empty. please check ray version or internal error",
                DeprecationWarning,
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
        if self.mode in set(["create", "overwrite"]):
            op = lance.LanceOperation.Overwrite(schema, fragments)
        elif self.mode == "append":
            op = lance.LanceOperation.Append(fragments)
        lance.LanceDataset.commit(
            self.uri,
            op,
            read_version=self.read_version,
            storage_options=self.storage_options,
        )


# ==============================================================================
# Fragment Writer and Committer
# ==============================================================================
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

        # Use default retry params similar to LanceDatasink
        retry_params = {
            "description": "write lance fragments",
            "match": ["LanceError(IO)"],
            "max_attempts": 10,
            "max_backoff_s": 32,
        }

        fragments = _write_fragment(
            transformed,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            max_rows_per_group=self.max_rows_per_group,
            max_bytes_per_file=self.max_bytes_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
            retry_params=retry_params,
        )
        return pa.Table.from_pydict(
            {
                "fragment": [pickle.dumps(fragment) for fragment, _ in fragments],
                "schema": [pickle.dumps(schema) for _, schema in fragments],
            }
        )


class LanceCommitter(_BaseLanceDatasink):
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


