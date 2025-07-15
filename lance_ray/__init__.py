"""
Lance-Ray: Ray integration for Lance columnar format.

This package provides integration between Ray and Lance for distributed
columnar data processing.
"""

__version__ = "0.0.1"
__author__ = "Lance Ray Team"
__email__ = "team@lance-ray.dev"

# Main imports
from .io import add_columns, read_lance, scanner, write_lance

__all__ = [
    "read_lance",
    "write_lance",
    "add_columns",
    "scanner",
]
