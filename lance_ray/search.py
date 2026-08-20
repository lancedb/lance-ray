# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import concurrent.futures
import inspect
import logging
import math
import pickle
from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc
import ray
from lance.dataset import LanceDataset

from .field_path import canonical_field_path, resolve_arrow_field_path
from .pool import get_or_create_pool
from .utils import (
    get_namespace_kwargs,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

if TYPE_CHECKING:
    import lance

logger = logging.getLogger(__name__)


class _SearchPlan(NamedTuple):
    fragment_ids: list[int]
    index_segments: list[str]


class _SearchPlanAnalysis(NamedTuple):
    plan: _SearchPlan
    analysis: str


class _SearchPlanUnit(NamedTuple):
    fragment_ids: set[int]
    index_segments: list[str]
    weight: int


def _dataset_load_kwargs(
    storage_options: Optional[dict[str, Any]],
    namespace_kwargs: dict[str, Any],
    block_size: Optional[int],
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "storage_options": storage_options,
        **namespace_kwargs,
    }
    if block_size is not None:
        kwargs["block_size"] = block_size
    return kwargs


def _get_dataset_storage_options(dataset: LanceDataset) -> dict[str, Any]:
    try:
        return dataset.initial_storage_options or {}
    except AttributeError:
        return getattr(dataset, "_storage_options", None) or {}


def _get_fragment_id(fragment: Any) -> int:
    try:
        return fragment.fragment_id
    except AttributeError:
        return fragment.metadata.id


def _index_value(index: Any, name: str, default: Any = None) -> Any:
    if isinstance(index, dict):
        return index.get(name, default)
    return getattr(index, name, default)


def _segment_value(segment: Any, name: str, default: Any = None) -> Any:
    if isinstance(segment, dict):
        return segment.get(name, default)
    return getattr(segment, name, default)


def _select_vector_index(
    dataset: LanceDataset,
    *,
    column: str,
    index_name: Optional[str],
) -> Any | None:
    indices = dataset.describe_indices()
    for index in indices:
        name = _index_value(index, "name")
        field_names = _index_value(index, "field_names")
        if field_names is None:
            field_names = _index_value(index, "fields", [])

        if index_name is not None:
            if name == index_name:
                return index
            continue

        if column in _canonical_index_field_names(field_names):
            return index

    if index_name is not None:
        available_names = [str(_index_value(index, "name")) for index in indices]
        raise ValueError(
            f"Vector index '{index_name}' was not found. "
            f"Available indices: {available_names}"
        )

    return None


def _canonical_index_field_names(field_names: Any) -> set[str]:
    canonical_names = set()
    for field_name in field_names or []:
        try:
            canonical_names.add(canonical_field_path(str(field_name)))
        except ValueError:
            canonical_names.add(str(field_name))
    return canonical_names


def _build_vector_search_plan_units(
    *,
    fragments: list[Any],
    vector_index: Any | None,
    include_unindexed: bool,
) -> tuple[list[_SearchPlanUnit], list[_SearchPlanUnit], int, int]:
    fragment_ids = {_get_fragment_id(fragment) for fragment in fragments}
    if not fragment_ids:
        return [], [], 0, 0

    fragment_weights: dict[int, int] = {}
    for fragment in fragments:
        fragment_id = _get_fragment_id(fragment)
        try:
            fragment_weights[fragment_id] = fragment.count_rows()
        except Exception:  # pragma: no cover - defensive fallback
            fragment_weights[fragment_id] = 1

    indexed_units: list[_SearchPlanUnit] = []
    fallback_units: list[_SearchPlanUnit] = []
    indexed_fragment_ids: set[int] = set()

    if vector_index is not None:
        for segment in _index_value(vector_index, "segments", []):
            segment_fragment_ids = set(_segment_value(segment, "fragment_ids", set()))
            segment_fragment_ids &= fragment_ids
            if not segment_fragment_ids:
                continue
            segment_uuid = str(_segment_value(segment, "uuid"))
            indexed_fragment_ids.update(segment_fragment_ids)
            indexed_units.append(
                _SearchPlanUnit(
                    fragment_ids=segment_fragment_ids,
                    index_segments=[segment_uuid],
                    weight=sum(fragment_weights[fid] for fid in segment_fragment_ids),
                )
            )

    fallback_fragment_ids = fragment_ids - indexed_fragment_ids
    if include_unindexed:
        for fragment_id in fallback_fragment_ids:
            fallback_units.append(
                _SearchPlanUnit(
                    fragment_ids={fragment_id},
                    index_segments=[],
                    weight=fragment_weights[fragment_id],
                )
            )

    return (
        indexed_units,
        fallback_units,
        len(fragment_ids),
        len(fallback_fragment_ids),
    )


def _plan_vector_search(
    *,
    fragments: list[Any],
    vector_index: Any | None,
    num_workers: int,
    include_unindexed: bool,
) -> list[_SearchPlan]:
    indexed_units, fallback_units, fragment_count, fallback_count = (
        _build_vector_search_plan_units(
            fragments=fragments,
            vector_index=vector_index,
            include_unindexed=include_unindexed,
        )
    )
    plans = [
        *_pack_search_plan_units(indexed_units, num_workers),
        *_pack_search_plan_units(fallback_units, num_workers),
    ]

    if not plans:
        return []

    included_fallback_count = fallback_count if include_unindexed else 0
    logger.info(
        "Planned distributed vector search across %d tasks, %d fragments, "
        "%d index segments, %d fallback fragments",
        len(plans),
        fragment_count,
        sum(len(plan.index_segments) for plan in plans),
        included_fallback_count,
    )
    return plans


def _pack_search_plan_units(
    units: list[_SearchPlanUnit],
    num_workers: int,
) -> list[_SearchPlan]:
    if not units:
        return []

    plan_count = min(num_workers, len(units))
    worker_fragment_ids: list[set[int]] = [set() for _ in range(plan_count)]
    worker_index_segments: list[list[str]] = [[] for _ in range(plan_count)]
    worker_weights = [0] * plan_count

    for unit in sorted(units, key=lambda item: item.weight, reverse=True):
        worker_idx = min(range(plan_count), key=lambda idx: worker_weights[idx])
        worker_fragment_ids[worker_idx].update(unit.fragment_ids)
        worker_index_segments[worker_idx].extend(unit.index_segments)
        worker_weights[worker_idx] += unit.weight

    plans = [
        _SearchPlan(
            fragment_ids=sorted(worker_fragment_ids[idx]),
            index_segments=worker_index_segments[idx],
        )
        for idx in range(plan_count)
        if worker_fragment_ids[idx]
    ]
    return plans


@lru_cache(maxsize=16)
def _load_pickled_dataset(pickled_dataset: bytes) -> LanceDataset:
    return pickle.loads(pickled_dataset)


@lru_cache(maxsize=16)
def _load_pickled_dataset_ref(pickled_dataset_ref: Any) -> LanceDataset:
    return _load_pickled_dataset(ray.get(pickled_dataset_ref))


def _load_worker_dataset(pickled_dataset: Any) -> LanceDataset:
    if isinstance(pickled_dataset, ray.ObjectRef):
        return _load_pickled_dataset_ref(pickled_dataset)
    return _load_pickled_dataset(pickled_dataset)


def _share_pickled_dataset_for_workers(pickled_dataset: bytes) -> tuple[Any, bool]:
    if not ray.is_initialized():
        return pickled_dataset, False
    return ray.put(pickled_dataset), True


def _execute_vector_search_plan(
    plan: _SearchPlan,
    *,
    pickled_dataset: Any,
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
    analyze_plan: bool,
) -> pa.Table | _SearchPlanAnalysis:
    dataset = _load_worker_dataset(pickled_dataset)

    if not plan.index_segments:
        return _execute_flat_fallback_vector_search_plan(
            dataset,
            plan=plan,
            base_scanner_options=base_scanner_options,
            nearest=nearest,
            candidate_k=candidate_k,
            analyze_plan=analyze_plan,
        )

    if not _scanner_accepts_index_segments(dataset):
        raise RuntimeError(
            "The installed pylance scanner does not support index_segments, "
            "which is required for distributed indexed vector search plans. "
            "Upgrade pylance or run without an indexed plan."
        )

    scanner_options = dict(base_scanner_options)
    search_nearest = dict(nearest)
    search_nearest["k"] = candidate_k

    scanner_options["nearest"] = search_nearest
    scanner_options["index_segments"] = plan.index_segments
    scanner_options["fast_search"] = True

    logger.info(
        "Running indexed vector search plan: fragments=%d, index_segments=%d, k=%d",
        len(plan.fragment_ids),
        len(plan.index_segments),
        candidate_k,
    )
    scanner = dataset.scanner(**scanner_options)
    if analyze_plan:
        return _SearchPlanAnalysis(plan=plan, analysis=scanner.analyze_plan())
    return scanner.to_table()


def _scanner_accepts_index_segments(dataset: LanceDataset) -> bool:
    try:
        parameters = inspect.signature(dataset.scanner).parameters
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return True
    return "index_segments" in parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )


def _execute_flat_fallback_vector_search_plan(
    dataset: LanceDataset,
    *,
    plan: _SearchPlan,
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
    analyze_plan: bool,
) -> pa.Table | _SearchPlanAnalysis:
    vector_column = nearest["column"]
    vector_scan_column, drop_vector_column = _prepare_fallback_scan_columns(
        base_scanner_options,
        vector_column,
    )
    scanner_options = dict(base_scanner_options)
    scanner_options.pop("fast_search", None)
    scanner_options["fragments"] = [
        dataset.get_fragment(fragment_id) for fragment_id in plan.fragment_ids
    ]

    logger.info(
        "Running flat fallback vector search plan: fragments=%d, k=%d",
        len(plan.fragment_ids),
        candidate_k,
    )
    scanner = dataset.scanner(**scanner_options)
    if analyze_plan:
        return _SearchPlanAnalysis(plan=plan, analysis=scanner.analyze_plan())

    table = scanner.to_table()
    if table.num_rows == 0:
        table = table.append_column("_distance", pa.array([], type=pa.float32()))
        if drop_vector_column and vector_scan_column in table.column_names:
            table = table.drop_columns([vector_scan_column])
        return table

    distances = _compute_vector_distances(
        table[vector_scan_column],
        nearest["q"],
        _get_nearest_metric(nearest),
    )
    table = table.append_column("_distance", pa.array(distances, type=pa.float32()))
    table = _take_top_k(table, candidate_k)
    if drop_vector_column and vector_scan_column in table.column_names:
        table = table.drop_columns([vector_scan_column])
    return table


def _prepare_fallback_scan_columns(
    scanner_options: dict[str, Any],
    vector_column: str,
    *,
    virtual_columns: Optional[set[str]] = None,
) -> tuple[str, bool]:
    requested_columns = scanner_options.get("columns")
    if requested_columns is None:
        return vector_column, False

    if isinstance(requested_columns, list):
        virtual_columns = virtual_columns or {"_distance"}
        scan_columns = [
            column for column in requested_columns if column not in virtual_columns
        ]
        if vector_column in scan_columns:
            scanner_options["columns"] = scan_columns
            return vector_column, False
        scanner_options["columns"] = [*scan_columns, vector_column]
        return vector_column, True

    if isinstance(requested_columns, dict):
        vector_scan_column = _unique_hidden_vector_column(requested_columns)
        scanner_options["columns"] = {
            **requested_columns,
            vector_scan_column: vector_column,
        }
        return vector_scan_column, True

    return vector_column, False


def _unique_hidden_vector_column(columns: dict[str, str]) -> str:
    vector_column = "__lance_ray_vector_search_vector"
    while vector_column in columns:
        vector_column = f"_{vector_column}"
    return vector_column


def _get_nearest_metric(nearest: dict[str, Any]) -> str:
    metric = nearest.get("metric") or nearest.get("distance_type") or "l2"
    return str(metric).lower()


def _compute_vector_distances(
    vector_column: pa.ChunkedArray,
    query: Any,
    metric: str,
) -> Any:
    import numpy as np

    matrix = _vector_column_to_numpy(vector_column)
    query_vector = np.asarray(query, dtype=np.float32)
    if query_vector.ndim != 1:
        raise ValueError("nearest['q'] must be a one-dimensional vector")
    if matrix.shape[1] != query_vector.shape[0]:
        raise ValueError(
            "Query vector dimension does not match fallback vector column "
            f"dimension: {query_vector.shape[0]} != {matrix.shape[1]}"
        )

    if metric in ("l2", "euclidean"):
        return np.linalg.norm(matrix - query_vector, axis=1).astype(np.float32)
    if metric == "cosine":
        query_norm = np.linalg.norm(query_vector)
        row_norms = np.linalg.norm(matrix, axis=1)
        denom = row_norms * query_norm
        similarities = np.divide(
            matrix @ query_vector,
            denom,
            out=np.zeros(matrix.shape[0], dtype=np.float32),
            where=denom != 0,
        )
        return (1.0 - similarities).astype(np.float32)
    if metric in ("dot", "ip", "inner_product"):
        return (-(matrix @ query_vector)).astype(np.float32)
    if metric == "hamming":
        return np.count_nonzero(matrix != query_vector, axis=1).astype(np.float32)

    raise ValueError(
        "Unsupported fallback vector search metric "
        f"{metric!r}. Supported metrics: l2, cosine, dot, hamming"
    )


def _vector_column_to_numpy(vector_column: pa.ChunkedArray) -> Any:
    import numpy as np

    values = vector_column.combine_chunks().to_pylist()
    if not values:
        return np.empty((0, 0), dtype=np.float32)
    if any(value is None for value in values):
        raise ValueError("Fallback vector search does not support null vectors")
    matrix = np.asarray(values, dtype=np.float32)
    if matrix.ndim != 2:
        raise ValueError("Fallback vector search requires a list-like vector column")
    return matrix


def _take_top_k(table: pa.Table, k: int) -> pa.Table:
    sort_indices = pc.sort_indices(table, sort_keys=[("_distance", "ascending")])
    return table.take(sort_indices.slice(0, k))


def _merge_vector_search_results(
    tables: list[pa.Table],
    k: int,
    *,
    per_query: bool = False,
    deterministic: bool = False,
) -> pa.Table:
    non_empty_tables = [table for table in tables if table.num_rows > 0]
    if not non_empty_tables:
        return tables[0].slice(0, 0) if tables else pa.table({})

    table = pa.concat_tables(non_empty_tables, promote_options="default")
    if "_distance" not in table.column_names:
        raise RuntimeError(
            "Distributed vector search results must include a '_distance' column "
            "for global top-k merge"
        )

    if per_query:
        if "query_index" not in table.column_names:
            raise RuntimeError(
                "Distributed batch vector search results must include a "
                "'query_index' column for per-query top-k merge"
            )
        return _take_top_k_per_query(table, k)
    if deterministic:
        return _take_top_k_deterministic(table, k)
    return _take_top_k(table, k)


def _format_analyze_plan_results(results: list[_SearchPlanAnalysis]) -> str:
    sections = []
    for idx, result in enumerate(results):
        plan_kind = "indexed" if result.plan.index_segments else "flat_fallback"
        sections.append(
            "\n".join(
                [
                    f"== Lance-Ray vector search shard {idx} ({plan_kind}) ==",
                    f"fragments: {result.plan.fragment_ids}",
                    f"index_segments: {result.plan.index_segments}",
                    result.analysis,
                ]
            )
        )
    return "\n\n".join(sections)


def _validate_search_scanner_options(scanner_options: dict[str, Any]) -> None:
    reserved_options = {
        "fast_search",
        "fragments",
        "index_segments",
        "nearest",
        "limit",
        "offset",
    }
    conflicts = sorted(reserved_options & scanner_options.keys())
    if conflicts:
        raise ValueError(
            "scanner_options cannot include distributed search managed options: "
            + ", ".join(conflicts)
        )


def _candidate_k(nearest: dict[str, Any], oversample_factor: float) -> tuple[int, int]:
    try:
        global_k = int(nearest["k"])
    except KeyError as exc:
        raise ValueError(
            "nearest must include 'k' for distributed vector search"
        ) from exc

    if global_k <= 0:
        raise ValueError(f"nearest['k'] must be positive, got {global_k}")
    if oversample_factor < 1:
        raise ValueError(
            f"oversample_factor must be greater than or equal to 1, got {oversample_factor}"
        )

    return global_k, max(global_k, math.ceil(global_k * oversample_factor))


def vector_search(
    uri: Optional[Union[str, "lance.LanceDataset"]] = None,
    *,
    nearest: dict[str, Any],
    index_name: Optional[str] = None,
    columns: Optional[list[str] | dict[str, str]] = None,
    filter: Optional[Any] = None,
    storage_options: Optional[dict[str, Any]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    num_workers: int = 4,
    ray_remote_args: Optional[dict[str, Any]] = None,
    oversample_factor: float = 1.0,
    include_unindexed: bool = True,
    fast_search: bool = False,
    analyze_plan: bool = False,
    scanner_options: Optional[dict[str, Any]] = None,
) -> pa.Table | str:
    """Run a distributed Lance vector search and merge the global top-k.

    The driver opens a fixed dataset version, plans ownership by vector index
    segment coverage.  Indexed worker tasks search only their assigned
    ``index_segments``.  Unindexed fallback tasks scan their assigned fragments
    without ``nearest`` and compute distances locally.  Workers return local
    candidates and the driver sorts by ``_distance`` to produce the final top-k
    table.

    Args:
        uri: Lance dataset object or dataset URI.  In URI mode, provide either
            ``uri`` or namespace parameters (``namespace_impl`` + ``table_id``).
        nearest: Lance vector search options.  Must include ``column``, ``q``,
            and ``k``.  The worker-side ``k`` is raised to at least
            ``k * oversample_factor`` before the driver performs the final
            global top-k merge.
        index_name: Optional vector index name to use.  If specified and the
            index cannot be found, ``ValueError`` is raised.  If omitted,
            Lance-Ray uses the first vector index covering ``nearest["column"]``.
        columns: Projection passed to the Lance scanner.  When a list is
            provided, ``_distance`` is appended automatically because the driver
            needs it to merge global top-k results.
        filter: Filter passed to every worker scanner.
        storage_options: Storage options used to open the dataset.  In namespace
            mode these are merged with namespace-provided storage options.
        block_size: Optional block size in bytes used when loading the dataset.
        namespace_impl: Namespace implementation type, such as ``"dir"`` or
            ``"rest"``.
        namespace_properties: Properties used to connect to the namespace.
        table_id: Table identifier used with namespace parameters.
        num_workers: Maximum number of Ray Pool workers to use.
        ray_remote_args: Ray remote options for Pool workers, such as
            ``num_cpus`` or custom resources.
        oversample_factor: Multiplier for local worker candidates.  Each worker
            returns at least ``nearest["k"] * oversample_factor`` rows before
            driver-side merge.  Must be greater than or equal to 1.
        include_unindexed: Include fragments not covered by vector index
            segments using separate flat-search fallback plans.  Fallback plans
            use regular fragment scans and compute vector distance in Lance-Ray.
            Ignored when ``fast_search=True``.
        fast_search: Search only indexed data.  When enabled, Lance-Ray does
            not schedule flat-search fallback plans for unindexed fragments.
        analyze_plan: Return Lance scanner analyze plans instead of executing
            the query and returning a table.  The result is a string containing
            one section per planned shard.
        scanner_options: Additional Lance scanner options.  Lance-Ray manages
            ``nearest``, ``fragments``, ``index_segments``, ``fast_search``,
            ``limit``, and ``offset`` internally, so these options cannot be
            supplied here.

    Returns:
        A PyArrow table containing the global top-k rows sorted by ``_distance``.
        If ``analyze_plan=True``, returns a string containing per-shard Lance
        scanner analysis instead.
    """
    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")
    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")

    column = nearest.get("column")
    if not column:
        raise ValueError("nearest must include 'column' for distributed vector search")

    global_k, candidate_k = _candidate_k(nearest, oversample_factor)

    base_scanner_options = dict(scanner_options or {})
    _validate_search_scanner_options(base_scanner_options)
    if columns is not None:
        if isinstance(columns, list) and "_distance" not in columns:
            columns = [*columns, "_distance"]
        base_scanner_options["columns"] = columns
    if filter is not None:
        base_scanner_options["filter"] = filter
    base_scanner_options["fast_search"] = fast_search

    merged_storage_options: dict[str, Any] = {}
    if storage_options:
        merged_storage_options.update(storage_options)

    if isinstance(uri, str | type(None)):
        validate_uri_or_namespace(uri, namespace_impl, table_id)
        uri, merged_storage_options = resolve_namespace_table(
            uri, storage_options, namespace_impl, namespace_properties, table_id
        )

        dataset_uri = uri
        namespace_kwargs = get_namespace_kwargs(
            namespace_impl, namespace_properties, table_id
        )
        dataset = LanceDataset(
            dataset_uri,
            **_dataset_load_kwargs(
                merged_storage_options, namespace_kwargs, block_size
            ),
        )
    else:
        dataset = uri
        if not merged_storage_options:
            merged_storage_options.update(_get_dataset_storage_options(dataset))

    try:
        resolved_column = resolve_arrow_field_path(dataset.schema, column)
    except KeyError as exc:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(
            f"Column '{column}' not found. Available: {available_columns}"
        ) from exc
    column = resolved_column.path
    nearest = {**nearest, "column": column}

    fragments = dataset.get_fragments()
    if not fragments:
        return pa.table({})

    vector_index = _select_vector_index(
        dataset,
        column=column,
        index_name=index_name,
    )
    if vector_index is None:
        logger.info(
            "No vector index found for column '%s'; distributed search will use flat scan",
            column,
        )

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=vector_index,
        num_workers=num_workers,
        include_unindexed=include_unindexed and not fast_search,
    )
    if not plans:
        return pa.table({})

    pickled_dataset = pickle.dumps(dataset)

    try:
        with get_or_create_pool(
            processes=min(num_workers, len(plans)),
            ray_remote_args=ray_remote_args,
        ) as pool:
            worker_pickled_dataset, _ = _share_pickled_dataset_for_workers(
                pickled_dataset
            )

            def run_plan(plan: _SearchPlan) -> pa.Table | _SearchPlanAnalysis:
                return _execute_vector_search_plan(
                    plan,
                    pickled_dataset=worker_pickled_dataset,
                    base_scanner_options=base_scanner_options,
                    nearest=nearest,
                    candidate_k=candidate_k,
                    analyze_plan=analyze_plan,
                )

            results = pool.map_async(run_plan, plans, chunksize=1).get()
    except Exception as exc:  # pragma: no cover - exercised via integration tests
        raise RuntimeError(
            f"Failed to complete distributed vector search: {exc}"
        ) from exc

    if analyze_plan:
        return _format_analyze_plan_results(results)

    return _merge_vector_search_results(results, global_k)


def _apply_index_metric_default(
    nearest: dict[str, Any],
    vector_index: Any | None,
) -> dict[str, Any]:
    if (
        vector_index is None
        or nearest.get("metric") is not None
        or nearest.get("distance_type") is not None
    ):
        return nearest

    details = _index_value(vector_index, "details", {}) or {}
    metric = _index_value(details, "metric_type")
    if metric is None:
        return nearest
    return {**nearest, "metric": str(metric).lower()}


def _inspect_vector_search_query(
    dataset: LanceDataset,
    *,
    nearest: dict[str, Any],
    base_scanner_options: dict[str, Any],
    include_row_id: bool,
) -> tuple[bool, pa.Schema]:
    probe = dataset.scanner(columns=["_distance"], nearest=nearest)
    probe_schema = probe.projected_schema
    is_batch_query = (
        probe_schema.names
        and probe_schema.names[0] == "query_index"
        and pa.types.is_int32(probe_schema.field(0).type)
        and not probe_schema.field(0).nullable
    )

    schema_options = dict(base_scanner_options)
    schema_options["nearest"] = nearest
    schema_options["with_row_id"] = True
    result_schema = dataset.scanner(**schema_options).projected_schema
    if not include_row_id:
        row_id_indices = result_schema.get_all_field_indices("_rowid")
        if row_id_indices:
            result_schema = result_schema.remove(row_id_indices[-1])

    return is_batch_query, result_schema


def _projection_includes_row_id(
    columns: Optional[list[str] | dict[str, str]],
    scanner_options: dict[str, Any],
) -> bool:
    if scanner_options.get("with_row_id"):
        return True
    if columns is None:
        columns = scanner_options.get("columns")
    if isinstance(columns, list):
        return "_rowid" in columns
    if isinstance(columns, dict):
        return "_rowid" in columns
    return False


def _compute_core_vector_distances(
    matrix: Any,
    query: Any,
    metric: str,
) -> Any:
    """Compute distances using Lance Core's current scalar conventions.

    The established ``vector_search`` fallback has different public distance
    conventions, so it continues to use ``_compute_vector_distances``.
    """
    import numpy as np

    dtype = np.uint8 if metric == "hamming" else np.float32
    query_vector = np.asarray(query, dtype=dtype)
    if query_vector.ndim != 1:
        raise ValueError("nearest['q'] must be a one-dimensional vector")
    if matrix.shape[1] != query_vector.shape[0]:
        raise ValueError(
            "Query vector dimension does not match fallback vector column "
            f"dimension: {query_vector.shape[0]} != {matrix.shape[1]}"
        )

    if metric in ("l2", "euclidean"):
        difference = matrix - query_vector
        return np.sum(difference * difference, axis=1).astype(np.float32)
    if metric == "cosine":
        query_norm = np.linalg.norm(query_vector)
        row_norms = np.linalg.norm(matrix, axis=1)
        denom = row_norms * query_norm
        similarities = np.full(matrix.shape[0], np.nan, dtype=np.float32)
        similarities = np.divide(
            matrix @ query_vector,
            denom,
            out=similarities,
            where=denom != 0,
        )
        return (1.0 - similarities).astype(np.float32)
    if metric in ("dot", "ip", "inner_product"):
        return (1.0 - matrix @ query_vector).astype(np.float32)
    if metric == "hamming":
        xor = np.bitwise_xor(matrix, query_vector)
        return np.bitwise_count(xor).sum(axis=1).astype(np.float32)

    raise ValueError(
        "Unsupported fallback vector search metric "
        f"{metric!r}. Supported metrics: l2, cosine, dot, hamming"
    )


def _vector_column_to_numpy_for_metric(
    vector_column: pa.ChunkedArray, metric: str
) -> Any:
    import numpy as np

    values = vector_column.combine_chunks().to_pylist()
    if not values:
        dtype = np.uint8 if metric == "hamming" else np.float32
        return np.empty((0, 0), dtype=dtype)
    dtype = np.uint8 if metric == "hamming" else np.float32
    matrix = np.asarray(values, dtype=dtype)
    if matrix.ndim != 2:
        raise ValueError("Fallback vector search requires a list-like vector column")
    return matrix


def _take_top_k_deterministic(table: pa.Table, k: int) -> pa.Table:
    sort_keys = [("_distance", "ascending")]
    if "_rowid" in table.column_names:
        sort_keys.append(("_rowid", "ascending"))
    sort_indices = pc.sort_indices(table, sort_keys=sort_keys)
    return table.take(sort_indices.slice(0, k))


def _apply_distance_range(table: pa.Table, nearest: dict[str, Any]) -> pa.Table:
    distance_range = nearest.get("distance_range")
    if distance_range is None:
        return table

    lower_bound, upper_bound = distance_range
    if lower_bound is not None:
        table = table.filter(pc.greater_equal(table["_distance"], lower_bound))
    if upper_bound is not None:
        table = table.filter(pc.less(table["_distance"], upper_bound))
    return table


def _take_top_k_per_query(table: pa.Table, k: int) -> pa.Table:
    import numpy as np

    sort_keys = [("query_index", "ascending"), ("_distance", "ascending")]
    if "_rowid" in table.column_names:
        sort_keys.append(("_rowid", "ascending"))
    sort_indices = pc.sort_indices(table, sort_keys=sort_keys)
    table = table.take(sort_indices)
    if table.num_rows == 0:
        return table

    query_indices = table["query_index"].combine_chunks().to_numpy()
    row_indices = np.arange(table.num_rows)
    group_starts = np.empty(table.num_rows, dtype=np.int64)
    group_starts[0] = 0
    group_starts[1:] = np.where(
        query_indices[1:] != query_indices[:-1],
        row_indices[1:],
        0,
    )
    np.maximum.accumulate(group_starts, out=group_starts)
    return table.filter(pa.array(row_indices - group_starts < k))


@dataclass(frozen=True)
class VectorSearchStreamingOptions:
    """Controls query batching and the bounded driver pipeline."""

    query_batch_size: Optional[int] = None
    max_in_flight_batches: int = 1

    def __post_init__(self) -> None:
        if self.query_batch_size is not None and self.query_batch_size <= 0:
            raise ValueError("query_batch_size must be positive")
        if self.max_in_flight_batches <= 0:
            raise ValueError("max_in_flight_batches must be positive")


@dataclass(frozen=True)
class VectorSearchActorOptions:
    """Controls Ray actors, their Lance sessions, and scanner execution."""

    num_actors: int = 4
    ray_remote_args: Optional[dict[str, Any]] = None
    max_concurrent_batches: int = 1
    max_pending_calls: Optional[int] = None
    micro_batch_size: Optional[int] = None
    scanner_concurrency: int = 1
    index_cache_size_bytes: Optional[int] = None
    metadata_cache_size_bytes: Optional[int] = None
    prewarm_index: bool = False

    def __post_init__(self) -> None:
        if self.num_actors <= 0:
            raise ValueError("num_actors must be positive")
        if self.max_concurrent_batches <= 0:
            raise ValueError("max_concurrent_batches must be positive")
        if self.max_pending_calls is not None and self.max_pending_calls <= 0:
            raise ValueError("max_pending_calls must be positive")
        if self.micro_batch_size is not None and self.micro_batch_size <= 0:
            raise ValueError("micro_batch_size must be positive")
        if self.scanner_concurrency <= 0:
            raise ValueError("scanner_concurrency must be positive")
        if self.index_cache_size_bytes is not None and self.index_cache_size_bytes < 0:
            raise ValueError("index_cache_size_bytes must be non-negative")
        if (
            self.metadata_cache_size_bytes is not None
            and self.metadata_cache_size_bytes < 0
        ):
            raise ValueError("metadata_cache_size_bytes must be non-negative")


@dataclass(frozen=True)
class _DatasetSnapshot:
    uri: str
    version: int
    serialized_manifest: bytes
    storage_options: dict[str, Any]
    base_store_params: Optional[dict[str, dict[str, Any]]]
    block_size: Optional[int]
    namespace_impl: Optional[str]
    namespace_properties: Optional[dict[str, str]]
    table_id: Optional[list[str]]


@dataclass(frozen=True)
class _ActorPlan:
    indexed_fragment_ids: tuple[int, ...]
    index_segments: tuple[str, ...]
    fallback_fragment_ids: tuple[int, ...]
    weight: int


def _plan_streaming_vector_search(
    *,
    fragments: list[Any],
    vector_index: Any | None,
    num_actors: int,
    fast_search: bool,
) -> list[_ActorPlan]:
    indexed_units, fallback_units, _, _ = _build_vector_search_plan_units(
        fragments=fragments,
        vector_index=vector_index,
        include_unindexed=not fast_search,
    )
    units = [*indexed_units, *fallback_units]

    if not units:
        return []

    actor_count = min(num_actors, len(units))
    actor_weights = [0] * actor_count
    indexed_fragments = [set() for _ in range(actor_count)]
    index_segments = [[] for _ in range(actor_count)]
    fallback_fragments = [set() for _ in range(actor_count)]

    for unit in sorted(units, key=lambda item: item.weight, reverse=True):
        actor_idx = min(range(actor_count), key=lambda idx: actor_weights[idx])
        if not unit.index_segments:
            fallback_fragments[actor_idx].update(unit.fragment_ids)
        else:
            indexed_fragments[actor_idx].update(unit.fragment_ids)
            index_segments[actor_idx].extend(unit.index_segments)
        actor_weights[actor_idx] += unit.weight

    return [
        _ActorPlan(
            indexed_fragment_ids=tuple(sorted(indexed_fragments[idx])),
            index_segments=tuple(index_segments[idx]),
            fallback_fragment_ids=tuple(sorted(fallback_fragments[idx])),
            weight=actor_weights[idx],
        )
        for idx in range(actor_count)
    ]


def _open_snapshot(
    snapshot: _DatasetSnapshot,
    *,
    index_cache_size_bytes: Optional[int],
    metadata_cache_size_bytes: Optional[int],
) -> LanceDataset:
    import lance

    session = lance.Session(
        index_cache_size_bytes=index_cache_size_bytes,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )
    namespace_kwargs = get_namespace_kwargs(
        snapshot.namespace_impl,
        snapshot.namespace_properties,
        snapshot.table_id,
    )
    kwargs: dict[str, Any] = {
        "storage_options": snapshot.storage_options,
        "session": session,
        **namespace_kwargs,
    }
    if snapshot.block_size is not None:
        kwargs["block_size"] = snapshot.block_size
    if snapshot.base_store_params is not None:
        kwargs["base_store_params"] = snapshot.base_store_params

    dataset = LanceDataset(
        snapshot.uri,
        version=snapshot.version,
        serialized_manifest=snapshot.serialized_manifest,
        **kwargs,
    )
    if dataset.version != snapshot.version:
        raise RuntimeError(
            f"Dataset snapshot changed: expected {snapshot.version}, "
            f"opened {dataset.version}"
        )
    return dataset


def _empty_fallback_table(
    scanner: Any,
    *,
    vector_column: str,
    drop_vector_column: bool,
) -> pa.Table:
    schema = getattr(scanner, "projected_schema", pa.schema([]))
    fields = list(schema)
    if drop_vector_column:
        fields = [field for field in fields if field.name != vector_column]
    fields = [
        field for field in fields if field.name not in {"query_index", "_distance"}
    ]
    schema = pa.schema(
        [
            pa.field("query_index", pa.int32(), nullable=False),
            *fields,
            pa.field("_distance", pa.float32()),
        ]
    )
    return pa.Table.from_batches([], schema=schema)


def _scanner_batches(scanner: Any) -> Iterable[pa.RecordBatch]:
    if hasattr(scanner, "to_batches"):
        return scanner.to_batches()
    return scanner.to_table().to_batches()


def _stream_flat_fallback(
    dataset: LanceDataset,
    *,
    fragment_ids: tuple[int, ...],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
) -> pa.Table:
    vector_column = nearest["column"]
    scanner_options = dict(base_scanner_options)
    vector_scan_column, drop_vector_column = _prepare_fallback_scan_columns(
        scanner_options,
        vector_column,
        virtual_columns={"_distance", "query_index"},
    )
    scanner_options.pop("fast_search", None)
    scanner_options["fragments"] = [
        dataset.get_fragment(fragment_id) for fragment_id in fragment_ids
    ]
    scanner = dataset.scanner(**scanner_options)

    metric = _get_nearest_metric(nearest)
    query_vectors = _canonical_query_batch(nearest["q"], metric, copy=False)
    running: Optional[pa.Table] = None
    for batch in _scanner_batches(scanner):
        table = pa.Table.from_batches([batch])
        if table.num_rows == 0:
            continue
        table = table.filter(pc.invert(pc.is_null(table[vector_scan_column])))
        if table.num_rows == 0:
            continue

        vector_matrix = _vector_column_to_numpy_for_metric(
            table[vector_scan_column], metric
        )
        query_results = []
        for query_index, query_vector in enumerate(query_vectors):
            import numpy as np

            distances = _compute_core_vector_distances(
                vector_matrix,
                query_vector,
                metric,
            )
            finite = np.isfinite(distances)
            query_result = table.filter(pa.array(finite, type=pa.bool_()))
            query_result = query_result.append_column(
                "_distance",
                pa.array(distances[finite], type=pa.float32()),
            )
            query_result = _apply_distance_range(query_result, nearest)
            query_result = _take_top_k_deterministic(query_result, candidate_k)
            if drop_vector_column and vector_scan_column in query_result.column_names:
                query_result = query_result.drop_columns([vector_scan_column])
            query_result = query_result.add_column(
                0,
                pa.field("query_index", pa.int32(), nullable=False),
                pa.array(
                    [query_index] * query_result.num_rows,
                    type=pa.int32(),
                ),
            )
            query_results.append(query_result)

        current = pa.concat_tables(query_results, promote_options="default")
        running = (
            current
            if running is None
            else _merge_vector_search_results(
                [running, current],
                candidate_k,
                per_query=True,
            )
        )

    if running is not None:
        return running
    return _empty_fallback_table(
        scanner,
        vector_column=vector_scan_column,
        drop_vector_column=drop_vector_column,
    )


def _indexed_search(
    dataset: LanceDataset,
    *,
    index_segments: tuple[str, ...],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
) -> pa.Table:
    if not _scanner_accepts_index_segments(dataset):
        raise RuntimeError(
            "The installed pylance scanner does not support index_segments"
        )
    scanner_options = dict(base_scanner_options)
    search_nearest = dict(nearest)
    search_nearest["k"] = candidate_k
    scanner_options.update(
        nearest=search_nearest,
        index_segments=index_segments,
        fast_search=True,
    )
    return dataset.scanner(**scanner_options).to_table()


def _offset_query_index(
    table: pa.Table,
    offset: int,
    *,
    output_type: pa.DataType,
) -> pa.Table:
    if "query_index" not in table.column_names:
        raise RuntimeError("Batch search result is missing query_index")
    values = pc.cast(table["query_index"], output_type)
    if offset:
        values = pc.add(values, pa.scalar(offset, output_type))
    return table.set_column(
        table.schema.get_field_index("query_index"),
        pa.field("query_index", output_type, nullable=False),
        values,
    )


def _canonical_query_batch(
    query: Any,
    metric: str,
    *,
    copy: bool = True,
) -> Any:
    import numpy as np

    dtype = np.uint8 if metric == "hamming" else np.float32
    if isinstance(query, pa.RecordBatch | pa.Table):
        if len(query.column_names) != 1:
            raise ValueError(
                "Arrow query batches must contain exactly one vector column"
            )
        query = query.column(0)
    if isinstance(query, pa.ChunkedArray):
        query = query.combine_chunks()
    if isinstance(query, pa.Array):
        query = query.to_pylist()
    if copy:
        array = np.array(query, dtype=dtype, copy=True, order="C")
    else:
        array = np.asarray(query, dtype=dtype, order="C")
    if array.size == 0:
        if array.ndim == 2:
            return array
        return np.empty((0, 0), dtype=dtype)
    if array.ndim == 1:
        array = array.reshape(1, -1)
    if array.ndim != 2:
        raise ValueError("Each query batch must be a two-dimensional array")
    return array


def _streaming_is_multivector_type(data_type: pa.DataType) -> bool:
    if not (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)):
        return False
    return pa.types.is_fixed_size_list(data_type.value_type)


def _canonical_multivector_batch(
    query: Any,
    metric: str,
) -> tuple[Any, ...]:
    import numpy as np

    dtype = np.uint8 if metric == "hamming" else np.float32
    if isinstance(query, pa.RecordBatch | pa.Table):
        if len(query.column_names) != 1:
            raise ValueError(
                "Arrow query batches must contain exactly one multivector column"
            )
        query = query.column(0)
    if isinstance(query, pa.ChunkedArray):
        query = query.combine_chunks()
    if isinstance(query, pa.Array):
        query = query.to_pylist()

    try:
        array = np.asarray(query, dtype=dtype)
    except ValueError:
        array = None

    if array is not None and array.ndim <= 3:
        if array.size == 0:
            return ()
        if array.ndim == 1:
            return (np.array(array.reshape(1, -1), copy=True, order="C"),)
        if array.ndim == 2:
            return (np.array(array, copy=True, order="C"),)
        return tuple(np.array(item, copy=True, order="C") for item in array)

    queries = []
    for item in query:
        item_array = np.array(item, dtype=dtype, copy=True, order="C")
        if item_array.ndim == 1:
            item_array = item_array.reshape(1, -1)
        if item_array.ndim != 2:
            raise ValueError("Each multivector query must have shape [M, D]")
        queries.append(item_array)
    return tuple(queries)


def _add_query_index(table: pa.Table, query_index: int) -> pa.Table:
    return table.add_column(
        0,
        pa.field("query_index", pa.int32(), nullable=False),
        pa.array([query_index] * table.num_rows, type=pa.int32()),
    )


def _multivector_fallback_search(
    dataset: LanceDataset,
    *,
    fragment_ids: tuple[int, ...],
    base_scanner_options: dict[str, Any],
    nearest: dict[str, Any],
    candidate_k: int,
) -> pa.Table:
    query = nearest["q"]
    scanner_options = dict(base_scanner_options)
    scanner_options.pop("fast_search", None)
    scanner_options["fragments"] = [
        dataset.get_fragment(fragment_id) for fragment_id in fragment_ids
    ]
    # Core requires prefilter for nearest scans scoped to explicit fragments.
    scanner_options["prefilter"] = True

    search_nearest = {**nearest, "k": candidate_k}
    distance_range = search_nearest.pop("distance_range", None)
    scanner_options["nearest"] = search_nearest
    table = dataset.scanner(**scanner_options).to_table()

    query_count = len(query)
    if query_count > 1 and table.num_rows:
        # Remove this compatibility offset once the minimum Core version uses
        # M - sum(MaxSim) for flat multivector distance.
        distances = pc.add(
            table["_distance"],
            pa.scalar(float(query_count - 1), pa.float32()),
        )
        table = table.set_column(
            table.schema.get_field_index("_distance"),
            pa.field("_distance", pa.float32()),
            distances,
        )
    if distance_range is not None:
        table = _apply_distance_range(
            table,
            {"distance_range": distance_range},
        )
    return _take_top_k_deterministic(table, candidate_k)


@ray.remote
class _VectorSearchActor:
    def __init__(
        self,
        snapshot: _DatasetSnapshot,
        plan: _ActorPlan,
        base_scanner_options: dict[str, Any],
        index_name: Optional[str],
        is_multivector: bool,
        actor_options: VectorSearchActorOptions,
    ):
        self._dataset = _open_snapshot(
            snapshot,
            index_cache_size_bytes=actor_options.index_cache_size_bytes,
            metadata_cache_size_bytes=actor_options.metadata_cache_size_bytes,
        )
        self._plan = plan
        self._base_scanner_options = base_scanner_options
        self._index_name = index_name
        self._is_multivector = is_multivector
        self._actor_options = actor_options

    def ready(self) -> dict[str, Any]:
        return {
            "version": self._dataset.version,
            "index_segments": len(self._plan.index_segments),
            "fallback_fragments": len(self._plan.fallback_fragment_ids),
        }

    def prewarm(self) -> dict[str, Any]:
        if not self._plan.index_segments or self._index_name is None:
            return {"index_segments": 0, "skipped": True}
        before = self._dataset.io_stats_snapshot()
        self._dataset.prewarm_index(
            self._index_name,
            index_segments=self._plan.index_segments,
        )
        after = self._dataset.io_stats_snapshot()
        session = self._dataset.session()
        return {
            "index_segments": len(self._plan.index_segments),
            "skipped": False,
            "cache_entries": self._dataset._ds.index_cache_entry_count(),
            "cache_size_bytes": session.index_cache_size_bytes(),
            "cache_hit_rate": self._dataset._ds.index_cache_hit_rate(),
            "read_bytes": after.read_bytes - before.read_bytes,
            "read_iops": after.read_iops - before.read_iops,
        }

    def search(
        self,
        query_batch: Any,
        nearest: dict[str, Any],
        candidate_k: int,
    ) -> pa.Table:
        metric = _get_nearest_metric(nearest)
        if self._is_multivector:
            queries = _canonical_multivector_batch(query_batch, metric)
        else:
            queries = _canonical_query_batch(query_batch, metric, copy=False)
        micro_batch_size = self._actor_options.micro_batch_size or len(queries)
        batches = [
            queries[offset : offset + micro_batch_size]
            for offset in range(0, len(queries), micro_batch_size)
        ]

        if self._actor_options.scanner_concurrency == 1:
            results = [
                self._search_micro_batch(batch, nearest, candidate_k)
                for batch in batches
            ]
        else:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._actor_options.scanner_concurrency
            ) as pool:
                results = list(
                    pool.map(
                        lambda batch: self._search_micro_batch(
                            batch,
                            nearest,
                            candidate_k,
                        ),
                        batches,
                    )
                )

        offset = 0
        adjusted = []
        for batch, result in zip(batches, results, strict=True):
            adjusted.append(_offset_query_index(result, offset, output_type=pa.int32()))
            offset += len(batch)
        return pa.concat_tables(adjusted, promote_options="default")

    def _search_micro_batch(
        self,
        query_batch: Any,
        nearest: dict[str, Any],
        candidate_k: int,
    ) -> pa.Table:
        if self._is_multivector:
            results = [
                _add_query_index(
                    self._search_multivector_query(query, nearest, candidate_k),
                    query_index,
                )
                for query_index, query in enumerate(query_batch)
            ]
            return pa.concat_tables(results, promote_options="default")

        search_nearest = {**nearest, "q": query_batch}
        tables = []
        if self._plan.index_segments:
            tables.append(
                _indexed_search(
                    self._dataset,
                    index_segments=self._plan.index_segments,
                    base_scanner_options=self._base_scanner_options,
                    nearest=search_nearest,
                    candidate_k=candidate_k,
                )
            )
        if self._plan.fallback_fragment_ids:
            tables.append(
                _stream_flat_fallback(
                    self._dataset,
                    fragment_ids=self._plan.fallback_fragment_ids,
                    base_scanner_options=self._base_scanner_options,
                    nearest=search_nearest,
                    candidate_k=candidate_k,
                )
            )
        return _merge_vector_search_results(
            tables,
            candidate_k,
            per_query=True,
        )

    def _search_multivector_query(
        self,
        query: Any,
        nearest: dict[str, Any],
        candidate_k: int,
    ) -> pa.Table:
        search_nearest = {**nearest, "q": query}
        tables = []
        if self._plan.index_segments:
            tables.append(
                _indexed_search(
                    self._dataset,
                    index_segments=self._plan.index_segments,
                    base_scanner_options=self._base_scanner_options,
                    nearest=search_nearest,
                    candidate_k=candidate_k,
                )
            )
        if self._plan.fallback_fragment_ids:
            tables.append(
                _multivector_fallback_search(
                    self._dataset,
                    fragment_ids=self._plan.fallback_fragment_ids,
                    base_scanner_options=self._base_scanner_options,
                    nearest=search_nearest,
                    candidate_k=candidate_k,
                )
            )
        return _merge_vector_search_results(
            tables,
            candidate_k,
            deterministic=True,
        )


class VectorSearchSession:
    """A snapshot-pinned, actor-backed streaming vector search session."""

    def __init__(
        self,
        *,
        dataset: LanceDataset,
        vector_type: pa.DataType,
        snapshot: _DatasetSnapshot,
        nearest: dict[str, Any],
        index_name: Optional[str],
        plans: list[_ActorPlan],
        base_scanner_options: dict[str, Any],
        include_row_id: bool,
        global_k: int,
        candidate_k: int,
        streaming_options: VectorSearchStreamingOptions,
        actor_options: VectorSearchActorOptions,
    ):
        self._dataset = dataset
        self.vector_type = vector_type
        self._is_multivector = _streaming_is_multivector_type(vector_type)
        self._nearest = nearest
        self._base_scanner_options = base_scanner_options
        self._include_row_id = include_row_id
        self._global_k = global_k
        self._candidate_k = candidate_k
        self._streaming_options = streaming_options
        self._result_names: Optional[list[str]] = None
        self._closed = False
        self.actor_states: list[dict[str, Any]] = []
        self.prewarm_results: list[dict[str, Any]] = []

        remote_args = dict(actor_options.ray_remote_args or {})
        remote_args.setdefault("num_cpus", 1)
        remote_args["max_concurrency"] = actor_options.max_concurrent_batches
        if actor_options.max_pending_calls is not None:
            remote_args["max_pending_calls"] = actor_options.max_pending_calls
        actor_class = _VectorSearchActor.options(**remote_args)
        self._actors = [
            actor_class.remote(
                snapshot,
                plan,
                base_scanner_options,
                index_name,
                self._is_multivector,
                actor_options,
            )
            for plan in plans
        ]
        try:
            if self._actors:
                self.actor_states = ray.get(
                    [actor.ready.remote() for actor in self._actors]
                )
            if actor_options.prewarm_index and self._actors:
                self.prewarm_results = ray.get(
                    [actor.prewarm.remote() for actor in self._actors]
                )
        except Exception:
            self.close()
            raise

    def __enter__(self) -> "VectorSearchSession":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for actor in self._actors:
            ray.kill(actor, no_restart=True)
        self._actors.clear()

    def map_batches(self, query_batches: Iterable[Any]) -> Iterator[pa.Table]:
        """Search an iterable of query batches with bounded memory.

        The driver canonicalizes each input batch, places it in Ray's object
        store once, broadcasts the resulting reference to all search actors,
        merges their local candidates per query, and yields the completed
        global top-k table. At most
        ``streaming_options.max_in_flight_batches`` batches are retained.

        Args:
            query_batches: Iterable of regular vector batches shaped ``[B, D]``
                or multivector batches described by :func:`open_vector_search`.

        Yields:
            PyArrow tables in input-batch order. ``query_index`` is an Int64
            position in the complete stream, not an index local to the batch.
        """
        if self._closed:
            raise RuntimeError("VectorSearchSession is closed")

        pending: deque[tuple[int, Any, list[Any]]] = deque()
        global_offset = 0

        def complete_one() -> pa.Table:
            offset, query_batch, refs = pending.popleft()
            tables = ray.get(refs) if refs else []
            result = self._finish_batch(
                tables,
                query_batch=query_batch,
                global_offset=offset,
            )
            return result

        for query_batch in self._iter_batches(query_batches):
            query_count = len(query_batch)
            if query_count == 0:
                continue
            while len(pending) >= self._streaming_options.max_in_flight_batches:
                yield complete_one()

            query_ref = ray.put(query_batch)
            refs = [
                actor.search.remote(
                    query_ref,
                    self._nearest,
                    self._candidate_k,
                )
                for actor in self._actors
            ]
            pending.append((global_offset, query_batch, refs))
            global_offset += query_count

        while pending:
            yield complete_one()

    def _iter_batches(self, query_batches: Iterable[Any]) -> Iterator[Any]:
        import numpy as np

        metric = _get_nearest_metric(self._nearest)
        target = self._streaming_options.query_batch_size
        if self._is_multivector:
            buffered_queries = []
            for query_batch in query_batches:
                canonical = _canonical_multivector_batch(query_batch, metric)
                if target is None:
                    yield canonical
                    continue
                buffered_queries.extend(canonical)
                while len(buffered_queries) >= target:
                    yield tuple(buffered_queries[:target])
                    del buffered_queries[:target]
            if buffered_queries:
                yield tuple(buffered_queries)
            return

        buffered = []
        buffered_rows = 0
        for query_batch in query_batches:
            canonical = _canonical_query_batch(query_batch, metric)
            if target is None:
                yield canonical
                continue
            offset = 0
            while offset < len(canonical):
                take = min(target - buffered_rows, len(canonical) - offset)
                buffered.append(canonical[offset : offset + take])
                buffered_rows += take
                offset += take
                if buffered_rows == target:
                    yield np.concatenate(buffered, axis=0)
                    buffered.clear()
                    buffered_rows = 0
        if buffered:
            yield np.concatenate(buffered, axis=0)

    def _finish_batch(
        self,
        tables: list[pa.Table],
        *,
        query_batch: Any,
        global_offset: int,
    ) -> pa.Table:
        schema_query = query_batch[0] if self._is_multivector else query_batch
        _, result_schema = _inspect_vector_search_query(
            self._dataset,
            nearest={**self._nearest, "q": schema_query},
            base_scanner_options=self._base_scanner_options,
            include_row_id=self._include_row_id,
        )
        if self._is_multivector:
            result_schema = pa.schema(
                [
                    pa.field("query_index", pa.int32(), nullable=False),
                    *result_schema,
                ]
            )

        if tables:
            result = _merge_vector_search_results(
                tables,
                self._global_k,
                per_query=True,
            )
        else:
            result = pa.Table.from_batches([], schema=result_schema)

        if self._result_names is None:
            self._result_names = result_schema.names
        result = _offset_query_index(
            result,
            global_offset,
            output_type=pa.int64(),
        )
        if not self._include_row_id and "_rowid" in result.column_names:
            result = result.drop_columns(["_rowid"])
        return result.select(self._result_names)


def _build_driver_dataset(
    uri: str | LanceDataset | None,
    *,
    storage_options: Optional[dict[str, Any]],
    base_store_params: Optional[dict[str, dict[str, Any]]],
    block_size: Optional[int],
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
    branch: Optional[str],
    version: int | str | None,
) -> tuple[LanceDataset, _DatasetSnapshot]:
    if branch is not None and version is not None:
        raise ValueError("branch and version are mutually exclusive")

    merged_storage_options = dict(storage_options or {})
    if isinstance(uri, LanceDataset):
        dataset = uri
        dataset_uri = dataset.uri
        if branch is not None:
            dataset = dataset.checkout_version((branch, None))
            dataset_uri = (
                f"{dataset_uri.partition('/tree/')[0].rstrip('/')}/tree/{branch}"
            )
        elif version is not None:
            dataset = dataset.checkout_version(version)
        if not merged_storage_options:
            merged_storage_options.update(_get_dataset_storage_options(dataset))
    else:
        validate_uri_or_namespace(uri, namespace_impl, table_id)
        dataset_uri, merged_storage_options = resolve_namespace_table(
            uri,
            storage_options,
            namespace_impl,
            namespace_properties,
            table_id,
        )
        kwargs: dict[str, Any] = {
            "storage_options": merged_storage_options,
            **get_namespace_kwargs(
                namespace_impl,
                namespace_properties,
                table_id,
            ),
        }
        if block_size is not None:
            kwargs["block_size"] = block_size
        if base_store_params is not None:
            kwargs["base_store_params"] = base_store_params
        dataset = LanceDataset(dataset_uri, **kwargs)
        if branch is not None:
            dataset = dataset.checkout_version((branch, None))
            dataset_uri = f"{dataset_uri.rstrip('/')}/tree/{branch}"
        elif version is not None:
            dataset = dataset.checkout_version(version)

    snapshot = _DatasetSnapshot(
        uri=dataset_uri,
        version=dataset.version,
        serialized_manifest=dataset._ds.serialized_manifest(),
        storage_options=merged_storage_options,
        base_store_params=base_store_params,
        block_size=block_size,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
    )
    return dataset, snapshot


def open_vector_search(
    uri: str | LanceDataset | None = None,
    *,
    nearest: dict[str, Any],
    index_name: Optional[str] = None,
    columns: Optional[list[str] | dict[str, str]] = None,
    filter: Optional[Any] = None,
    storage_options: Optional[dict[str, Any]] = None,
    base_store_params: Optional[dict[str, dict[str, Any]]] = None,
    block_size: Optional[int] = None,
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[dict[str, str]] = None,
    table_id: Optional[list[str]] = None,
    branch: Optional[str] = None,
    version: int | str | None = None,
    oversample_factor: float = 1.0,
    fast_search: bool = False,
    scanner_options: Optional[dict[str, Any]] = None,
    streaming_options: Optional[VectorSearchStreamingOptions] = None,
    actor_options: Optional[VectorSearchActorOptions] = None,
) -> VectorSearchSession:
    """Open a reusable distributed vector search session.

    The session pins the dataset manifest when it opens, assigns index segments
    and uncovered fragments to persistent Ray actors, and reuses each actor's
    Lance session and index cache across query batches. Use the returned object
    as a context manager so the actors are stopped when the stream finishes.

    Queries are supplied later through :meth:`VectorSearchSession.map_batches`;
    do not include ``q`` in ``nearest``. For a fixed-size vector column, each
    input batch is an array with shape ``[B, D]``. For a multivector column, an
    input batch is a sequence of ``[M_i, D]`` arrays, an Arrow
    ``List<FixedSizeList<D>>`` array, or a ``[B, M, D]`` array when ``M`` is
    fixed. Every output table contains an Int64 ``query_index`` that identifies
    the query's position across the entire input stream.

    Args:
        uri: Lance dataset object or dataset URI. In URI mode, provide either
            ``uri`` or namespace parameters (``namespace_impl`` + ``table_id``).
            An already checked-out dataset retains its exact manifest.
        nearest: Lance nearest-neighbor options without ``q``. ``column`` and
            ``k`` are required. Options such as ``nprobes``,
            ``query_parallelism``, ``approx_mode``, ``refine_factor``, and
            ``distance_range`` are forwarded to Lance Core.
        index_name: Optional vector index name. If omitted, the first vector
            index covering ``nearest["column"]`` is selected.
        columns: Columns or projection expressions returned for each match.
            ``_distance`` is added when needed for distributed top-k merging.
        filter: Filter passed to each actor's Lance scanner.
        storage_options: Storage options used to open the dataset.
        base_store_params: Runtime options for registered external base paths.
        block_size: Optional dataset I/O block size in bytes.
        namespace_impl: Namespace implementation, such as ``"dir"`` or
            ``"rest"``.
        namespace_properties: Properties used to connect to the namespace.
        table_id: Table identifier used with namespace parameters.
        branch: Branch resolved and pinned when the session opens. Mutually
            exclusive with ``version``.
        version: Dataset version or tag to pin. Mutually exclusive with
            ``branch``.
        oversample_factor: Multiplier applied to each actor's local candidate
            count before the driver performs the global top-k merge.
        fast_search: If true, intentionally skip fragments not covered by the
            selected vector index. If false, include them through flat fallback.
        scanner_options: Additional Lance scanner options. ``nearest``,
            ``fragments``, ``index_segments``, ``fast_search``, ``limit``, and
            ``offset`` are managed by Lance-Ray and cannot be supplied here.
        streaming_options: Input rebatching and bounded in-flight pipeline
            settings.
        actor_options: Actor count, Ray resources, micro-batching, scanner
            concurrency, cache sizes, pending-call limits, and optional index
            prewarming.

    Returns:
        A snapshot-pinned :class:`VectorSearchSession`.

    Example:
        >>> with open_vector_search(
        ...     "dataset.lance",
        ...     nearest={"column": "vector", "k": 10, "nprobes": 8},
        ...     streaming_options=VectorSearchStreamingOptions(
        ...         query_batch_size=512,
        ...         max_in_flight_batches=2,
        ...     ),
        ... ) as search:
        ...     for result in search.map_batches(query_batches):
        ...         write_result(result)
    """
    streaming_options = streaming_options or VectorSearchStreamingOptions()
    actor_options = actor_options or VectorSearchActorOptions()

    if block_size is not None and block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")
    if "q" in nearest:
        raise ValueError("open_vector_search receives queries through map_batches")
    if not nearest.get("column"):
        raise ValueError("nearest must include 'column'")
    nearest = dict(nearest)
    global_k, candidate_k = _candidate_k(nearest, oversample_factor)

    base_scanner_options = dict(scanner_options or {})
    _validate_search_scanner_options(base_scanner_options)
    include_row_id = _projection_includes_row_id(columns, base_scanner_options)
    effective_columns = (
        columns if columns is not None else base_scanner_options.get("columns")
    )
    if effective_columns is not None:
        if (
            isinstance(effective_columns, list) and "query_index" in effective_columns
        ) or (
            isinstance(effective_columns, dict) and "query_index" in effective_columns
        ):
            raise ValueError(
                "query_index is managed by streaming vector search and cannot "
                "be included in columns"
            )
        if isinstance(effective_columns, list) and "_distance" not in effective_columns:
            effective_columns = [*effective_columns, "_distance"]
        base_scanner_options["columns"] = effective_columns
    if filter is not None:
        base_scanner_options["filter"] = filter
    base_scanner_options["with_row_id"] = True

    dataset, snapshot = _build_driver_dataset(
        uri,
        storage_options=storage_options,
        base_store_params=base_store_params,
        block_size=block_size,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        branch=branch,
        version=version,
    )
    if "query_index" in dataset.schema.names:
        raise ValueError(
            "Batch vector search cannot use a dataset containing column 'query_index'"
        )
    try:
        resolved_field = resolve_arrow_field_path(
            dataset.schema,
            nearest["column"],
        )
    except KeyError as exc:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(
            f"Column '{nearest['column']}' not found. Available: {available_columns}"
        ) from exc
    resolved_column = resolved_field.path
    nearest = {**nearest, "column": resolved_column}

    vector_index = _select_vector_index(
        dataset,
        column=resolved_column,
        index_name=index_name,
    )
    nearest = _apply_index_metric_default(nearest, vector_index)
    resolved_index_name = (
        str(_index_value(vector_index, "name")) if vector_index is not None else None
    )
    plans = _plan_streaming_vector_search(
        fragments=dataset.get_fragments(),
        vector_index=vector_index,
        num_actors=actor_options.num_actors,
        fast_search=fast_search,
    )

    return VectorSearchSession(
        dataset=dataset,
        vector_type=resolved_field.field.type,
        snapshot=snapshot,
        nearest=nearest,
        index_name=resolved_index_name,
        plans=plans,
        base_scanner_options=base_scanner_options,
        include_row_id=include_row_id,
        global_k=global_k,
        candidate_k=candidate_k,
        streaming_options=streaming_options,
        actor_options=actor_options,
    )


def _is_multivector_type(data_type: pa.DataType) -> bool:
    if not (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)):
        return False
    return (
        pa.types.is_fixed_size_list(data_type.value_type)
        or pa.types.is_list(data_type.value_type)
        or pa.types.is_large_list(data_type.value_type)
    )
