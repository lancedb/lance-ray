from types import SimpleNamespace

import lance
import lance_ray as lr
import numpy as np
import pyarrow as pa
import pytest
from lance_ray import pool as pool_mod
from lance_ray import search as search_mod
from lance_ray.search import (
    VectorSearchActorOptions,
    VectorSearchStreamingOptions,
    _apply_distance_range,
    _canonical_multivector_batch,
    _canonical_query_batch,
    _compute_core_vector_distances,
    _execute_vector_search_plan,
    _format_analyze_plan_results,
    _merge_vector_search_results,
    _plan_streaming_vector_search,
    _plan_vector_search,
    _SearchPlan,
    _SearchPlanAnalysis,
    _select_vector_index,
    _validate_search_scanner_options,
)


class _FakeFragment:
    def __init__(self, fragment_id: int, rows: int = 1):
        self.fragment_id = fragment_id
        self._rows = rows

    def count_rows(self):
        return self._rows


def _index_with_segments(*segments):
    return SimpleNamespace(
        name="vector_idx",
        field_names=["vector"],
        index_type="IVF_PQ",
        segments=[
            SimpleNamespace(uuid=uuid, fragment_ids=set(fragment_ids))
            for uuid, fragment_ids in segments
        ],
    )


def _vector_table(vectors, ids=None):
    matrix = np.asarray(vectors, dtype=np.float32)
    vector_array = pa.FixedSizeListArray.from_arrays(
        pa.array(matrix.reshape(-1), type=pa.float32()),
        matrix.shape[1],
    )
    return pa.table(
        {
            "id": range(len(matrix)) if ids is None else ids,
            "vector": vector_array,
        }
    )


def _mock_pickled_dataset(monkeypatch, dataset):
    search_mod._load_pickled_dataset.cache_clear()
    search_mod._load_pickled_dataset_ref.cache_clear()
    pickled_dataset = f"pickled-dataset-{id(dataset)}".encode()

    def fake_loads(value):
        assert value == pickled_dataset
        return dataset

    monkeypatch.setattr(search_mod.pickle, "loads", fake_loads)
    return pickled_dataset


def test_select_vector_index_raises_for_missing_explicit_index_name():
    index = _index_with_segments(("S1", [1, 2]))
    dataset = SimpleNamespace(describe_indices=lambda: [index])

    with pytest.raises(ValueError, match="missing_idx.*vector_idx"):
        _select_vector_index(
            dataset,
            column="vector",
            index_name="missing_idx",
        )


def test_select_vector_index_matches_canonical_lance_field_path():
    index = SimpleNamespace(
        name="hyphen_idx",
        field_names=["`meta-data`.`user-id`"],
        index_type="IVF_PQ",
        segments=[],
    )
    dataset = SimpleNamespace(describe_indices=lambda: [index])

    assert (
        _select_vector_index(
            dataset,
            column="`meta-data`.`user-id`",
            index_name=None,
        )
        is index
    )


def test_plan_vector_search_keeps_segment_fragments_together():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 6)]
    index = _index_with_segments(
        ("S1", [1, 2]),
        ("S2", [3]),
        ("S3", [4, 5]),
    )

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=3,
        include_unindexed=True,
    )

    segment_fragments = {
        segment: set(plan.fragment_ids)
        for plan in plans
        for segment in plan.index_segments
    }

    assert segment_fragments == {
        "S1": {1, 2},
        "S2": {3},
        "S3": {4, 5},
    }


def test_plan_vector_search_adds_unindexed_fragments_as_fallback():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 5)]
    index = _index_with_segments(("S1", [1, 2]))

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=3,
        include_unindexed=True,
    )

    fallback_fragments = {
        fragment_id
        for plan in plans
        if not plan.index_segments
        for fragment_id in plan.fragment_ids
    }

    assert fallback_fragments == {3, 4}
    assert all(
        not (plan.index_segments and fallback_fragments.intersection(plan.fragment_ids))
        for plan in plans
    )


def test_plan_vector_search_does_not_mix_indexed_and_fallback_units():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 5)]
    index = _index_with_segments(("S1", [1, 2]))

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=1,
        include_unindexed=True,
    )

    assert plans == [
        _SearchPlan(fragment_ids=[1, 2], index_segments=["S1"]),
        _SearchPlan(fragment_ids=[3, 4], index_segments=[]),
    ]


def test_plan_vector_search_can_skip_unindexed_fragments():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 5)]
    index = _index_with_segments(("S1", [1, 2]))

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=index,
        num_workers=3,
        include_unindexed=False,
    )

    assert plans == [_SearchPlan(fragment_ids=[1, 2], index_segments=["S1"])]


def test_plan_vector_search_without_index_uses_flat_fallback():
    fragments = [_FakeFragment(fragment_id) for fragment_id in range(1, 4)]

    plans = _plan_vector_search(
        fragments=fragments,
        vector_index=None,
        num_workers=2,
        include_unindexed=True,
    )

    assert {fragment_id for plan in plans for fragment_id in plan.fragment_ids} == {
        1,
        2,
        3,
    }
    assert all(not plan.index_segments for plan in plans)


def test_execute_indexed_vector_search_plan_does_not_pass_fragments(monkeypatch):
    scanner_options = {}

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            raise AssertionError(f"unexpected fragment lookup: {fragment_id}")

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return SimpleNamespace(
                to_table=lambda: pa.table({"id": [1], "_distance": [0.1]})
            )

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[1, 2], index_segments=["S1"]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
        base_scanner_options={"fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 1},
        candidate_k=1,
        analyze_plan=False,
    )

    assert result.num_rows == 1
    assert "fragments" not in scanner_options
    assert scanner_options["index_segments"] == ["S1"]
    assert scanner_options["fast_search"] is True


def test_execute_indexed_vector_search_plan_without_index_segments_support(monkeypatch):
    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def scanner(self, columns=None):
            return SimpleNamespace(to_table=lambda: pa.table({"id": [1]}))

    with pytest.raises(RuntimeError, match="index_segments"):
        _execute_vector_search_plan(
            _SearchPlan(fragment_ids=[1, 2], index_segments=["S1"]),
            pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
            base_scanner_options={"columns": ["id"], "fast_search": True},
            nearest={"column": "vector", "q": [0.0, 0.0], "k": 1},
            candidate_k=1,
            analyze_plan=False,
        )


def test_execute_fallback_vector_search_plan_computes_local_top_k(monkeypatch):
    scanner_options = {}
    vectors = pa.FixedSizeListArray.from_arrays(
        pa.array([10.0, 0.0, 1.0, 0.0, 0.0, 2.0], type=pa.float32()),
        2,
    )

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            return f"fragment-{fragment_id}"

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return SimpleNamespace(
                to_table=lambda: pa.table({"id": [1, 2, 3], "vector": vectors})
            )

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
        base_scanner_options={"columns": ["id", "_distance"], "fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 2},
        candidate_k=2,
        analyze_plan=False,
    )

    assert "nearest" not in scanner_options
    assert scanner_options["fragments"] == ["fragment-7"]
    assert scanner_options["columns"] == ["id", "vector"]
    assert result.column("id").to_pylist() == [2, 3]
    assert result.column("_distance").to_pylist() == [1.0, 2.0]
    assert "vector" not in result.column_names


def test_execute_indexed_vector_search_plan_can_analyze_plan(monkeypatch):
    scanner_options = {}

    class FakeScanner:
        def analyze_plan(self):
            return "indexed plan"

        def to_table(self):
            raise AssertionError("analyze_plan should not execute to_table")

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            raise AssertionError(f"unexpected fragment lookup: {fragment_id}")

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return FakeScanner()

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[1], index_segments=["S1"]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
        base_scanner_options={"fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 1},
        candidate_k=1,
        analyze_plan=True,
    )

    assert result == _SearchPlanAnalysis(
        plan=_SearchPlan(fragment_ids=[1], index_segments=["S1"]),
        analysis="indexed plan",
    )
    assert "fragments" not in scanner_options
    assert scanner_options["index_segments"] == ["S1"]
    assert scanner_options["fast_search"] is True


def test_execute_fallback_vector_search_plan_can_analyze_plan(monkeypatch):
    scanner_options = {}

    class FakeScanner:
        def analyze_plan(self):
            return "fallback plan"

        def to_table(self):
            raise AssertionError("analyze_plan should not execute to_table")

    class FakeDataset:
        def __init__(self, *args, **kwargs):
            pass

        def get_fragment(self, fragment_id):
            return f"fragment-{fragment_id}"

        def scanner(self, **kwargs):
            scanner_options.update(kwargs)
            return FakeScanner()

    result = _execute_vector_search_plan(
        _SearchPlan(fragment_ids=[7], index_segments=[]),
        pickled_dataset=_mock_pickled_dataset(monkeypatch, FakeDataset()),
        base_scanner_options={"columns": ["id", "_distance"], "fast_search": False},
        nearest={"column": "vector", "q": [0.0, 0.0], "k": 2},
        candidate_k=2,
        analyze_plan=True,
    )

    assert result == _SearchPlanAnalysis(
        plan=_SearchPlan(fragment_ids=[7], index_segments=[]),
        analysis="fallback plan",
    )
    assert "nearest" not in scanner_options
    assert scanner_options["fragments"] == ["fragment-7"]
    assert scanner_options["columns"] == ["id", "vector"]


def test_format_analyze_plan_results():
    result = _format_analyze_plan_results(
        [
            _SearchPlanAnalysis(
                plan=_SearchPlan(fragment_ids=[1], index_segments=["S1"]),
                analysis="indexed plan",
            ),
            _SearchPlanAnalysis(
                plan=_SearchPlan(fragment_ids=[2], index_segments=[]),
                analysis="fallback plan",
            ),
        ]
    )

    assert "shard 0 (indexed)" in result
    assert "index_segments: ['S1']" in result
    assert "indexed plan" in result
    assert "shard 1 (flat_fallback)" in result
    assert "fallback plan" in result


def test_merge_vector_search_results_returns_global_top_k():
    left = pa.table({"id": [1, 2], "_distance": [0.4, 0.1]})
    right = pa.table({"id": [3, 4], "_distance": [0.2, 0.3]})

    result = _merge_vector_search_results([left, right], k=3)

    assert result.column("id").to_pylist() == [2, 3, 4]
    assert result.column("_distance").to_pylist() == [0.1, 0.2, 0.3]


def test_merge_vector_search_results_requires_distance():
    table = pa.table({"id": [1, 2]})

    with pytest.raises(RuntimeError, match="_distance"):
        _merge_vector_search_results([table], k=1)


def test_merge_vector_search_results_can_merge_per_query():
    left = pa.table(
        {
            "query_index": [0, 0, 1],
            "id": [1, 2, 3],
            "_distance": [0.4, 0.1, 0.3],
        }
    )
    right = pa.table(
        {
            "query_index": [0, 1, 1],
            "id": [4, 5, 6],
            "_distance": [0.2, 0.4, 0.1],
        }
    )

    result = _merge_vector_search_results([left, right], k=2, per_query=True)

    assert result["query_index"].to_pylist() == [0, 0, 1, 1]
    assert result["id"].to_pylist() == [2, 4, 6, 3]


def test_search_scanner_options_reject_managed_options():
    with pytest.raises(ValueError, match="nearest"):
        _validate_search_scanner_options({"nearest": {"column": "vector"}})


def test_search_scanner_options_reject_fast_search_override():
    with pytest.raises(ValueError, match="fast_search"):
        _validate_search_scanner_options({"fast_search": True})


def test_vector_search_reuses_global_pool(monkeypatch):
    events = []

    class FakeAsyncResult:
        def get(self):
            events.append("get")
            return [pa.table({"id": [1], "_distance": [0.1]})]

    class FakeGlobalPool:
        def map_async(self, func, plans, chunksize):
            events.append(("map_async", plans, chunksize))
            return FakeAsyncResult()

        def close(self):
            events.append("close")

        def join(self):
            events.append("join")

    class FakeSchema:
        def field(self, column):
            return column

    class FakeDataset:
        uri = "dataset"
        version = 1
        schema = FakeSchema()

        def __init__(self, *args, **kwargs):
            pass

        def get_fragments(self):
            return [_FakeFragment(1)]

    plan = _SearchPlan(fragment_ids=[1], index_segments=["S1"])
    monkeypatch.setattr(search_mod, "LanceDataset", FakeDataset)
    monkeypatch.setattr(
        search_mod,
        "_select_vector_index",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(search_mod, "_plan_vector_search", lambda **kwargs: [plan])
    monkeypatch.setattr(search_mod.pickle, "dumps", lambda dataset: b"pickled-dataset")
    monkeypatch.setattr(search_mod.ray, "is_initialized", lambda: False)

    pool_mod.set_global_pool(FakeGlobalPool())
    try:
        result = search_mod.vector_search(
            uri="dataset",
            nearest={"column": "vector", "q": [0.0], "k": 1},
            num_workers=4,
        )
    finally:
        pool_mod.clear_global_pool()

    assert result.column("id").to_pylist() == [1]
    assert events == [
        ("map_async", [plan], 1),
        "get",
    ]


def test_streaming_option_defaults():
    assert VectorSearchStreamingOptions() == VectorSearchStreamingOptions(
        query_batch_size=None,
        max_in_flight_batches=1,
    )
    assert VectorSearchActorOptions() == VectorSearchActorOptions(
        num_actors=4,
        ray_remote_args=None,
        max_concurrent_batches=1,
        max_pending_calls=None,
        micro_batch_size=None,
        scanner_concurrency=1,
        index_cache_size_bytes=None,
        metadata_cache_size_bytes=None,
        prewarm_index=False,
    )
    assert VectorSearchActorOptions(
        index_cache_size_bytes=0,
        metadata_cache_size_bytes=0,
    )


def test_open_vector_search_requires_explicit_k():
    with pytest.raises(ValueError, match="nearest must include 'k'"):
        lr.open_vector_search(nearest={"column": "vector"})


@pytest.mark.parametrize(
    ("metric", "vectors", "query", "expected"),
    [
        ("l2", [[0.0, 2.0], [3.0, 4.0]], [0.0, 0.0], [4.0, 25.0]),
        (
            "cosine",
            [[1.0, 0.0], [1.0, 1.0]],
            [1.0, 0.0],
            [0.0, 0.29289323],
        ),
        ("dot", [[1.0, 0.0], [1.0, 1.0]], [1.0, 1.0], [0.0, -1.0]),
        (
            "hamming",
            [[0, 0], [255, 0], [15, 240], [1, 2]],
            [0, 0],
            [0.0, 8.0, 8.0, 2.0],
        ),
    ],
)
def test_streaming_fallback_distance_matches_core(metric, vectors, query, expected):
    dtype = np.uint8 if metric == "hamming" else np.float32

    distances = _compute_core_vector_distances(
        np.asarray(vectors, dtype=dtype),
        query,
        metric,
    )

    assert distances.tolist() == pytest.approx(expected)


def test_streaming_distance_range_is_lower_inclusive_upper_exclusive():
    table = pa.table({"id": [0, 1, 2], "_distance": [0.5, 1.0, 4.0]})

    result = _apply_distance_range(table, {"distance_range": (0.5, 4.0)})

    assert result["id"].to_pylist() == [0, 1]


@pytest.mark.parametrize(
    "columns",
    [
        ["id", "query_index"],
        {"query_index": "id"},
    ],
)
def test_open_vector_search_rejects_query_index_projection(columns):
    with pytest.raises(ValueError, match="query_index is managed"):
        lr.open_vector_search(
            nearest={"column": "vector", "k": 10},
            columns=columns,
        )


def test_open_vector_search_rejects_dataset_query_index_column(tmp_path):
    table = _vector_table([[0.0, 0.0], [1.0, 0.0]])
    table = table.append_column("query_index", pa.array([1, 2], type=pa.int32()))
    dataset = lance.write_dataset(table, tmp_path / "query-index.lance")

    with pytest.raises(ValueError, match="containing column 'query_index'"):
        lr.open_vector_search(
            dataset,
            nearest={"column": "vector", "k": 1},
        )


def test_streaming_query_batches_are_canonicalized_by_column_type():
    source = np.arange(12, dtype=np.float32).reshape(3, 4)[:, ::-1]
    regular = _canonical_query_batch(source, "l2")

    assert regular.flags.c_contiguous
    assert regular.flags.owndata
    assert not np.shares_memory(regular, source)

    multivector = _canonical_multivector_batch(
        [
            np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            np.asarray([[1.0, 1.0]], dtype=np.float32),
        ],
        "cosine",
    )
    assert [query.shape for query in multivector] == [(2, 2), (1, 2)]
    assert all(query.flags.c_contiguous for query in multivector)


def test_streaming_planner_balances_indexed_and_fallback_units():
    fragments = [
        _FakeFragment(1, 100),
        _FakeFragment(2, 90),
        _FakeFragment(3, 80),
    ]
    plans = _plan_streaming_vector_search(
        fragments=fragments,
        vector_index=_index_with_segments(("S1", [1]), ("S2", [2])),
        num_actors=2,
        fast_search=False,
    )

    assert len(plans) == 2
    assert {segment for plan in plans for segment in plan.index_segments} == {
        "S1",
        "S2",
    }
    assert {
        fragment_id for plan in plans for fragment_id in plan.fallback_fragment_ids
    } == {3}
    assert any(plan.index_segments and plan.fallback_fragment_ids for plan in plans)


def test_streaming_fallback_preserves_global_query_indices(tmp_path):
    dataset = lance.write_dataset(
        _vector_table(
            [
                [0.0, 0.0],
                [1.0, 0.0],
                [0.0, 2.0],
                [3.0, 0.0],
                [0.0, 4.0],
                [5.0, 0.0],
            ]
        ),
        tmp_path / "streaming-flat.lance",
        max_rows_per_file=2,
    )

    with lr.open_vector_search(
        dataset,
        nearest={"column": "vector", "k": 2},
        columns=["id"],
        actor_options=VectorSearchActorOptions(num_actors=2),
        streaming_options=VectorSearchStreamingOptions(max_in_flight_batches=2),
    ) as session:
        results = list(
            session.map_batches(
                [
                    np.asarray([[0.0, 0.0], [0.0, 4.0]], dtype=np.float32),
                    np.asarray([[3.0, 0.0]], dtype=np.float32),
                ]
            )
        )

    assert [result["query_index"].to_pylist() for result in results] == [
        [0, 0, 1, 1],
        [2, 2],
    ]
    assert results[0]["query_index"].type == pa.int64()
    assert results[0]["id"].to_pylist() == [0, 1, 4, 2]
    assert results[1]["id"].to_pylist() == [3, 1]


def test_streaming_fast_search_without_index_returns_empty_result(tmp_path):
    dataset = lance.write_dataset(
        _vector_table([[0.0, 0.0], [1.0, 0.0]]),
        tmp_path / "streaming-fast.lance",
    )

    with lr.open_vector_search(
        dataset,
        nearest={"column": "vector", "k": 1},
        columns=["id"],
        fast_search=True,
    ) as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[0.0, 0.0], [1.0, 0.0]], dtype=np.float32)]
            )
        )

    assert result.num_rows == 0
    assert result.column_names == ["query_index", "id", "_distance"]
    assert result["query_index"].type == pa.int64()


def test_streaming_partial_index_merges_fallback_results(tmp_path):
    path = tmp_path / "streaming-partial.lance"
    dataset = lance.write_dataset(
        _vector_table([[0.0, 0.0], [1.0, 0.0], [0.0, 2.0], [3.0, 0.0]]),
        path,
        max_rows_per_file=2,
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="vector_idx",
    )
    lance.write_dataset(
        _vector_table([[0.0, 4.0], [5.0, 0.0]], ids=[4, 5]),
        path,
        mode="append",
    )

    with lr.open_vector_search(
        lance.dataset(path),
        nearest={"column": "vector", "k": 2, "nprobes": 1},
        index_name="vector_idx",
        columns=["id"],
        actor_options=VectorSearchActorOptions(num_actors=2),
    ) as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[0.0, 4.0], [3.0, 0.0]], dtype=np.float32)]
            )
        )

    assert result["query_index"].to_pylist() == [0, 0, 1, 1]
    assert result["id"].to_pylist() == [4, 2, 3, 1]


def test_streaming_fallback_inherits_index_metric(tmp_path):
    path = tmp_path / "streaming-partial-cosine.lance"
    dataset = lance.write_dataset(
        _vector_table([[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [-1.0, 0.0]]),
        path,
        max_rows_per_file=2,
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="vector_idx",
        metric="cosine",
    )
    lance.write_dataset(
        _vector_table([[0.5, 0.5], [-1.0, -1.0]], ids=[4, 5]),
        path,
        mode="append",
    )

    with lr.open_vector_search(
        lance.dataset(path),
        nearest={"column": "vector", "k": 3, "nprobes": 1},
        index_name="vector_idx",
        columns=["id"],
        actor_options=VectorSearchActorOptions(num_actors=2),
    ) as session:
        [result] = list(
            session.map_batches(
                [np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)]
            )
        )

    assert result["query_index"].to_pylist() == [0, 0, 0, 1, 1, 1]
    assert result["id"].to_pylist() == [0, 1, 4, 2, 1, 4]
    assert result["_distance"].to_pylist() == pytest.approx(
        [0.0, 0.29289323, 0.29289323, 0.0, 0.29289323, 0.29289323]
    )


def test_streaming_can_prewarm_owned_index_segments(tmp_path):
    dataset = lance.write_dataset(
        _vector_table([[0.0, 0.0], [1.0, 0.0], [0.0, 2.0], [3.0, 0.0]]),
        tmp_path / "streaming-prewarm.lance",
        max_rows_per_file=2,
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="vector_idx",
    )

    with lr.open_vector_search(
        lance.dataset(dataset.uri),
        nearest={"column": "vector", "k": 1, "nprobes": 1},
        index_name="vector_idx",
        columns=["id"],
        actor_options=VectorSearchActorOptions(
            num_actors=1,
            prewarm_index=True,
        ),
    ) as session:
        assert session.prewarm_results[0]["skipped"] is False
        assert session.prewarm_results[0]["index_segments"] == 1
        [result] = list(
            session.map_batches([np.asarray([[0.0, 0.0]], dtype=np.float32)])
        )

    assert result["id"].to_pylist() == [0]


def test_streaming_inherits_checked_out_dataset_snapshot(tmp_path):
    path = tmp_path / "streaming-branch.lance"
    dataset = lance.write_dataset(_vector_table([[0.0, 0.0]], ids=[0]), path)
    branch_dataset = dataset.create_branch("experiment")
    lance.write_dataset(
        _vector_table([[1.0, 0.0]], ids=[1]),
        path,
        mode="append",
    )

    with lr.open_vector_search(
        branch_dataset,
        nearest={"column": "vector", "k": 2},
        columns=["id"],
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(
            session.map_batches([np.asarray([[0.0, 0.0]], dtype=np.float32)])
        )

    assert result["id"].to_pylist() == [0]
    assert session.actor_states[0]["version"] == branch_dataset.version


def test_streaming_uri_branch_uses_branch_snapshot(tmp_path):
    path = tmp_path / "streaming-uri-branch.lance"
    dataset = lance.write_dataset(_vector_table([[0.0, 0.0]], ids=[0]), path)
    branch_dataset = dataset.create_branch("experiment")
    branch_dataset = lance.write_dataset(
        _vector_table([[1.0, 0.0]], ids=[1]),
        branch_dataset.uri,
        mode="append",
    )

    assert lance.dataset(path).version < branch_dataset.version

    with lr.open_vector_search(
        str(path),
        branch="experiment",
        nearest={"column": "vector", "k": 2},
        columns=["id"],
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(
            session.map_batches([np.asarray([[0.0, 0.0]], dtype=np.float32)])
        )

    assert result["id"].to_pylist() == [0, 1]
    assert session.actor_states[0]["version"] == branch_dataset.version


def test_streaming_multivector_uses_additive_maxsim_distance(tmp_path):
    vector_type = pa.list_(pa.list_(pa.float32(), 2))
    dataset = lance.write_dataset(
        pa.table(
            {
                "id": [0, 1, 2],
                "vector": pa.array(
                    [
                        [[1.0, 0.0], [0.0, 1.0]],
                        [[1.0, 0.0]],
                        [[-1.0, 0.0], [0.0, -1.0]],
                    ],
                    type=vector_type,
                ),
            }
        ),
        tmp_path / "streaming-multivector.lance",
    )
    query_batch = pa.array(
        [
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0]],
        ],
        type=vector_type,
    )

    with lr.open_vector_search(
        dataset,
        nearest={"column": "vector", "k": 2, "metric": "cosine"},
        columns=["id"],
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(session.map_batches([query_batch]))

    assert result["query_index"].to_pylist() == [0, 0, 1, 1]
    assert result["id"].to_pylist() == [0, 1, 0, 1]
    assert result["_distance"].to_pylist() == pytest.approx([0.0, 1.0, 0.0, 0.0])


def test_streaming_multivector_uses_core_indexed_search(tmp_path):
    vector_type = pa.list_(pa.list_(pa.float32(), 2))
    rows = [
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0]],
        [[-1.0, 0.0], [0.0, -1.0]],
    ] * 20
    dataset = lance.write_dataset(
        pa.table(
            {
                "id": range(len(rows)),
                "vector": pa.array(rows, type=vector_type),
            }
        ),
        tmp_path / "streaming-multivector-indexed.lance",
    )
    dataset.create_index(
        "vector",
        "IVF_FLAT",
        num_partitions=1,
        name="multivector_idx",
        metric="cosine",
    )

    with lr.open_vector_search(
        lance.dataset(dataset.uri),
        nearest={"column": "vector", "k": 1, "nprobes": 1},
        index_name="multivector_idx",
        columns=["id"],
        actor_options=VectorSearchActorOptions(num_actors=1),
    ) as session:
        [result] = list(
            session.map_batches(
                [
                    pa.array(
                        [
                            [[1.0, 0.0], [0.0, 1.0]],
                            [[1.0, 0.0]],
                        ],
                        type=vector_type,
                    )
                ]
            )
        )

    assert result["query_index"].to_pylist() == [0, 1]
    assert result["_distance"].to_pylist() == pytest.approx([0.0, 0.0])


def test_vector_search_puts_pickled_dataset_in_ray_object_store(monkeypatch):
    events = []

    class FakeObjectRef:
        def __init__(self, value):
            self.value = value

    class FakeAsyncResult:
        def __init__(self, results):
            self._results = results

        def get(self):
            events.append("get")
            return self._results

    class FakeGlobalPool:
        def map_async(self, func, plans, chunksize):
            events.append(("map_async", plans, chunksize))
            return FakeAsyncResult([func(plan) for plan in plans])

        def close(self):
            events.append("close")

        def join(self):
            events.append("join")

    class FakeSchema:
        def field(self, column):
            return column

    class FakeDataset:
        schema = FakeSchema()

        def get_fragments(self):
            return [_FakeFragment(1), _FakeFragment(2)]

        def scanner(self, **kwargs):
            events.append(("scanner", kwargs))
            return SimpleNamespace(
                to_table=lambda: pa.table({"id": [1], "_distance": [0.1]})
            )

    fake_dataset = FakeDataset()
    pickled_dataset = b"pickled-dataset"

    def fake_lance_dataset(*args, **kwargs):
        events.append(("LanceDataset", args, kwargs))
        return fake_dataset

    def fake_dumps(dataset):
        events.append(("pickle.dumps", dataset is fake_dataset))
        return pickled_dataset

    def fake_put(value):
        events.append(("ray.put", value))
        return FakeObjectRef(value)

    def fake_get(ref):
        events.append(("ray.get", ref.value))
        return ref.value

    def fake_loads(value):
        events.append(("pickle.loads", value))
        assert value == pickled_dataset
        return fake_dataset

    plans = [
        _SearchPlan(fragment_ids=[1], index_segments=["S1"]),
        _SearchPlan(fragment_ids=[2], index_segments=["S2"]),
    ]

    monkeypatch.setattr(search_mod, "LanceDataset", fake_lance_dataset)
    monkeypatch.setattr(
        search_mod,
        "_select_vector_index",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(search_mod, "_plan_vector_search", lambda **kwargs: plans)
    monkeypatch.setattr(search_mod.pickle, "dumps", fake_dumps)
    monkeypatch.setattr(search_mod.pickle, "loads", fake_loads)
    monkeypatch.setattr(search_mod.ray, "ObjectRef", FakeObjectRef, raising=False)
    monkeypatch.setattr(search_mod.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(search_mod.ray, "put", fake_put)
    monkeypatch.setattr(search_mod.ray, "get", fake_get)
    search_mod._load_pickled_dataset.cache_clear()
    search_mod._load_pickled_dataset_ref.cache_clear()

    pool_mod.set_global_pool(FakeGlobalPool())
    try:
        result = search_mod.vector_search(
            uri="dataset",
            nearest={"column": "vector", "q": [0.0], "k": 1},
            num_workers=4,
        )
    finally:
        pool_mod.clear_global_pool()
        search_mod._load_pickled_dataset.cache_clear()
        search_mod._load_pickled_dataset_ref.cache_clear()

    assert result.column("id").to_pylist() == [1]
    assert events == [
        ("LanceDataset", ("dataset",), {"storage_options": {}}),
        ("pickle.dumps", True),
        ("ray.put", pickled_dataset),
        ("map_async", plans, 1),
        ("ray.get", pickled_dataset),
        ("pickle.loads", pickled_dataset),
        (
            "scanner",
            {
                "fast_search": True,
                "nearest": {"column": "vector", "q": [0.0], "k": 1},
                "index_segments": ["S1"],
            },
        ),
        (
            "scanner",
            {
                "fast_search": True,
                "nearest": {"column": "vector", "q": [0.0], "k": 1},
                "index_segments": ["S2"],
            },
        ),
        "get",
    ]
