from typing import TYPE_CHECKING, Any, Optional

import ray
from lance.dataset import LanceDataset, ScannerBuilder
from ray.data.dataset import Dataset
from ray.data.read_api import from_arrow_refs

if TYPE_CHECKING:
    from lance.dataset import QueryVectorLike


@ray.remote
class LanceRayScannerActor:
    def __init__(self, builder: ScannerBuilder):
        self._builder = builder
        self._lance_ds = builder.ds

    def to_table(self, fragment_ids: list[int]):
        fragments = [
            self._lance_ds.get_fragment(fragment_id) for fragment_id in fragment_ids
        ]
        builder = self._builder.with_fragments(fragments)
        return builder.to_scanner().to_table()


class LanceRayScanner:
    def __init__(
        self,
        builder: ScannerBuilder,
        lance_ds: LanceDataset,
        concurrency: Optional[int] = None,
        ray_remote_args: Optional[dict[str, Any]] = None,
    ):
        fragment_ids = [f.metadata.id for f in lance_ds.get_fragments()]
        self._fragment_groups = []
        if concurrency is not None and concurrency > 0:
            group_size = (len(fragment_ids) + concurrency - 1) // concurrency
            self._fragment_groups = [
                fragment_ids[i : i + group_size]
                for i in range(0, len(fragment_ids), group_size)
            ]
        else:
            self._fragment_groups = [fragment_ids]
            concurrency = 1
        create_handle = (
            LanceRayScannerActor.options(**ray_remote_args)
            if ray_remote_args is not None
            else LanceRayScannerActor
        )
        self._actor_pool = [create_handle.remote(builder) for _ in range(concurrency)]

    def to_dataset(self) -> Dataset:
        return from_arrow_refs(
            [
                actor.to_table.remote(fragment_ids)
                for actor, fragment_ids in zip(self._actor_pool, self._fragment_groups, strict=False)
            ]
        )

    def nearest(
        self,
        column: str,
        q: "QueryVectorLike",
        k: Optional[int] = None,
        metric: Optional[str] = None,
        nprobes: Optional[int] = None,
        minimum_nprobes: Optional[int] = None,
        maximum_nprobes: Optional[int] = None,
        refine_factor: Optional[int] = None,
        use_index: bool = True,
        ef: Optional[int] = None,
    ):
        pass


class RayScannerBuilder:
    def __init__(
        self,
        uri: str,
        *,
        storage_options: Optional[dict[str, Any]] = None,
        ray_remote_args: Optional[dict[str, Any]] = None,
        concurrency: Optional[int] = None,
        read_version: Optional[int | str] = None,
    ):
        self._ray_remote_args = ray_remote_args
        self._concurrency = concurrency
        self._lance_ds = LanceDataset(
            uri=uri, storage_options=storage_options, version=read_version
        )
        self._builder = ScannerBuilder(self._lance_ds)

    def __getattr__(self, name: str):
        """
        Dynamically handle method calls.
        If method name is 'to_scanner', call self._to_scanner.
        Otherwise, delegate to self._builder if the attribute exists and wrap the result.
        """
        if name == "to_scanner":
            return self._to_scanner
        elif hasattr(self._builder, name):
            attr = getattr(self._builder, name)
            if callable(attr):

                def wrapper(*args, **kwargs):
                    attr(*args, **kwargs)
                    return self

                return wrapper
            else:
                return attr
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    def _to_scanner(self) -> LanceRayScanner:
        return LanceRayScanner(
            self._builder,
            self._lance_ds,
            concurrency=self._concurrency,
            ray_remote_args=self._ray_remote_args,
        )
