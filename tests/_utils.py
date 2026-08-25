"""Shared helpers for tests."""

import inspect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def missing_fragment_write_options(*options: str) -> tuple[str, ...]:
    from lance.fragment import write_fragments

    params = inspect.signature(write_fragments).parameters
    return tuple(sorted(set(options).difference(params)))


def fragment_write_options_skip_reason(*options: str) -> str:
    missing = missing_fragment_write_options(*options)
    return (
        "Installed pylance does not expose the missing fragment write "
        "option(s) on lance.fragment.write_fragments: "
        f"{', '.join(missing)}"
    )


def to_numpy_backed(df: "pd.DataFrame") -> "pd.DataFrame":
    """Convert Arrow-backed columns of ``df`` back to numpy dtypes.

    Ray 2.56 made ``Dataset.to_pandas()`` map Arrow types onto
    ``pd.ArrowDtype`` (``DataContext.enable_arrow_backed_pandas_conversion``,
    on by default). That changes the dtypes a frame reports and surfaces nulls
    as ``pd.NA`` rather than ``None``, so assertions written against plain
    pandas frames and Python values normalise through this helper first.

    On Ray < 2.56 there is nothing to convert and ``df`` is returned unchanged,
    which keeps the assertions meaningful on both sides of that release.
    """
    import pyarrow as pa

    import pandas as pd

    if not any(isinstance(dtype, pd.ArrowDtype) for dtype in df.dtypes):
        return df

    table = pa.Table.from_pandas(df, preserve_index=False)
    # ``from_pandas`` records the Arrow-backed origin in the schema metadata and
    # ``to_pandas`` would faithfully restore it, so drop the metadata first.
    return table.replace_schema_metadata(None).to_pandas()
