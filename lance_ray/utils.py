import sys
from typing import TypeVar, Iterable, Sequence


if sys.version_info >= (3, 12):
    from itertools import batched

    T = TypeVar("T")

    def array_split(iterable: Iterable[T], n: int) -> list[Sequence[T]]:
        """Split iterable into n chunks."""
        items = list(iterable)
        chunk_size = (len(items) + n - 1) // n
        return list(batched(items, chunk_size))
else:
    from more_itertools import divide

    def array_split(iterable: Iterable[T], n: int) -> list[Sequence[T]]:
        return list(map(list, divide(n, iterable)))
