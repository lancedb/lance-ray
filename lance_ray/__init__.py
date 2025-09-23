"""
Lance-Ray: Ray integration for Lance columnar format.

This package provides integration between Ray and Lance for distributed
columnar data processing.
"""

__version__ = "0.0.5"
__author__ = "LanceDB Devs"
__email__ = "dev@lancedb.com"

# Main imports
from .index import create_scalar_index
from .io import add_columns, read_lance, write_lance

# Fragment API imports
from .fragment import (
    LanceFragmentWriter,
    LanceCommitter,
    execute_fragment_operation,
    add_columns as add_columns_distributed,
    DispatchFragmentTasks,
    FragmentTask,
    AddColumnTask,
    _register_hooks,
)

__all__ = [
    "read_lance",
    "write_lance",
    "add_columns",
    "create_scalar_index",
    "LanceFragmentWriter",
    "LanceCommitter",
    "execute_fragment_operation",
    "add_columns_distributed",
    "DispatchFragmentTasks",
    "FragmentTask",
    "AddColumnTask",
    "_register_hooks",
]
