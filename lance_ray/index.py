# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import logging
import re
import time
import uuid
from typing import Any, Optional, Union

import lance
import pyarrow as pa
from lance.dataset import Index, LanceDataset
from packaging import version
from ray.util.multiprocessing import Pool

logger = logging.getLogger(__name__)

def _distribute_fragments_balanced(
    fragments: list[Any], num_workers: int, logger: logging.Logger
) -> list[list[int]]:
    """
    Distribute fragments across workers using a balanced algorithm that considers fragment sizes.

    This function implements a greedy algorithm that assigns fragments to the worker
    with the currently smallest total workload, helping to balance the processing
    time across workers.

    Args:
        fragments: List of Lance fragment objects
        num_workers: Number of workers to distribute fragments across
        logger: Logger instance for debugging information

    Returns:
        List of lists, where each inner list contains fragment IDs for one worker
    """
    if not fragments:
        return [[] for _ in range(num_workers)]

    # Get fragment information (ID and size)
    fragment_info = []
    for fragment in fragments:
        try:
            # Try to get fragment size information
            # fragment.count_rows() gives us the number of rows in the fragment
            row_count = fragment.count_rows()
            fragment_info.append({
                "id": fragment.fragment_id,
                "size": row_count,
            })
        except Exception as e:
            # If we can't get size info, use fragment_id as a fallback
            logger.warning(
                f"Could not get size for fragment {fragment.fragment_id}: {e}. "
                "Using fragment_id as size estimate."
            )
            fragment_info.append({
                "id": fragment.fragment_id,
                "size": fragment.fragment_id,  # Fallback to fragment_id
            })

    # Sort fragments by size in descending order (largest first)
    # This helps with better load balancing using the greedy algorithm
    fragment_info.sort(key=lambda x: x["size"], reverse=True)

    # Initialize worker batches and their current workloads
    worker_batches = [[] for _ in range(num_workers)]
    worker_workloads = [0] * num_workers

    # Greedy assignment: assign each fragment to the worker with minimum workload
    for frag_info in fragment_info:
        # Find the worker with the minimum current workload
        min_workload_idx = min(range(num_workers), key=lambda i: worker_workloads[i])

        # Assign fragment to this worker
        worker_batches[min_workload_idx].append(frag_info["id"])
        worker_workloads[min_workload_idx] += frag_info["size"]

    # Log distribution statistics for debugging
    total_size = sum(frag_info["size"] for frag_info in fragment_info)
    logger.info("Fragment distribution statistics:")
    logger.info(f"  Total fragments: {len(fragment_info)}")
    logger.info(f"  Total size: {total_size}")
    logger.info(f"  Workers: {num_workers}")

    for i, (batch, workload) in enumerate(zip(worker_batches, worker_workloads, strict=False)):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info(
            f"  Worker {i}: {len(batch)} fragments, "
            f"workload: {workload} ({percentage:.1f}%)"
        )

    # Filter out empty batches (shouldn't happen with proper input validation)
    non_empty_batches = [batch for batch in worker_batches if batch]

    return non_empty_batches


def generate_default_index_name(column: str, index_type: str, dataset: Optional["lance.LanceDataset"] = None) -> str:
    """
    Generate a default index name based on column name and index type

    Args:
        column: The column name to base the index name on
        index_type: The type of index (e.g., "INVERTED", "FTS")
        dataset: Optional Lance dataset to check for existing indices

    Returns:
        A unique, valid index name
    """
    # Normalize the column name
    if not column:
        normalized_column = "column"
    else:
        # Replace invalid chars with underscores
        normalized = re.sub(r"[^a-zA-Z0-9_]", "_", column)

        # Collapse multiple underscores
        normalized = re.sub(r"_+", "_", normalized)

        # Remove leading and trailing underscores
        normalized = normalized.strip("_")

        # If empty after normalization or starts with a number, use default
        if not normalized or normalized[0].isdigit():
            normalized_column = "column"
        else:
            normalized_column = normalized

    # Normalize index type
    normalized_index_type = index_type.lower()

    # Build the base name
    base_name = f"{normalized_column}_{normalized_index_type}_idx"

    # Check for conflicts if dataset is provided
    if dataset is not None:
        try:
            existing_indices = dataset.list_indices()
            existing_names = {idx["name"] for idx in existing_indices}

            if base_name in existing_names:
                # Find the next available suffix number
                for i in range(2, 1000):
                    candidate = f"{base_name}_{i}"
                    if candidate not in existing_names:
                        return candidate

                # Use timestamp suffix if all numbered options are taken
                return f"{base_name}_{int(time.time())}"
        except Exception:
            # If we can't check existing indices, just return the base name
            pass

    return base_name


def _handle_fragment_index(
    dataset_uri: str,
    column: str,
    index_type: str,
    name: str,
    fragment_uuid: str,
    storage_options: Optional[dict[str, str]] = None,
    **kwargs: Any,
):
    """
    Create a function to handle fragment index building for use with Pool.
    This function returns a callable that can be used with Pool.map_async
    to build indices for specific fragments.
    """
    def func(fragment_ids: list[int]) -> dict[str, Any]:
        """
        Handle fragment index building using the distributed API.

        This function calls create_scalar_index directly for specific fragments.
        After execution, fragment-level indices are automatically built.

        Args:
            fragment_ids: List of fragment IDs to build index for

        Returns:
            Dictionary with status and result information
        """
        try:
            # Basic input validation
            if not fragment_ids:
                raise ValueError("fragment_ids cannot be empty")

            # Validate fragment_id ranges
            for fragment_id in fragment_ids:
                if fragment_id < 0 or fragment_id > 0xFFFFFFFF:
                    raise ValueError(f"Invalid fragment_id: {fragment_id}")

            # Load dataset
            dataset = LanceDataset(dataset_uri, storage_options=storage_options)

            # Validate fragments exist
            available_fragments = {f.fragment_id for f in dataset.get_fragments()}
            invalid_fragments = set(fragment_ids) - available_fragments
            if invalid_fragments:
                raise ValueError(f"Fragment IDs {invalid_fragments} do not exist")

            # Use the distributed index building API - Phase 1: Fragment index creation
            logger.info(f"Building distributed index for fragments {fragment_ids} using create_scalar_index")

            # Call create_scalar_index directly - no return value expected
            # After execution, fragment-level indices are automatically built
            dataset.create_scalar_index(
                column=column,
                index_type=index_type,
                name=name,
                replace=False,
                fragment_uuid=fragment_uuid,
                fragment_ids=fragment_ids,
                **kwargs
            )

            # Get field ID for the indexed column
            field_id = dataset.schema.get_field_index(column)

            logger.info(f"Fragment index created successfully for fragments {fragment_ids}")

            return {
                "status": "success",
                "fragment_ids": fragment_ids,
                "fields": [field_id],
                "uuid": fragment_uuid,
            }

        except Exception as e:
            logger.error(f"Fragment index task failed for fragments {fragment_ids}: {e}")
            return {
                "status": "error",
                "fragment_ids": fragment_ids,
                "error": str(e),
            }

    return func

def merge_index_metadata_compat(dataset, index_id, default_index_type="INVERTED"):
    try:
        return dataset.merge_index_metadata(index_id, default_index_type)
    except TypeError:
        return dataset.merge_index_metadata(index_id)

def create_scalar_index(
    dataset: Union[str, "lance.LanceDataset"],
    column: str,
    index_type: str,
    name: Optional[str] = None,
    num_workers: int = 4,
    storage_options: Optional[dict[str, str]] = None,
    ray_remote_args: Optional[dict[str, Any]] = None,
    **kwargs: Any,
) -> "lance.LanceDataset":
    """
    Build a distributed full-text search index using Ray.

    This function distributes the index building process across multiple Ray workers,
    with each worker building indices for a subset of fragments. The indices are then
    merged and committed as a single index.

    Args:
        dataset: Lance dataset or URI to build index on
        column: Column name to index
        index_type: Type of index to build ("INVERTED" or "FTS")
        name: Name of the index (generated if None)
        num_workers: Number of Ray workers to use
        storage_options: Storage options for the dataset
        ray_remote_args: Options for Ray tasks (e.g., num_cpus, resources)
        **kwargs: Additional arguments to pass to create_scalar_index

    Returns:
        Updated Lance dataset with the index created

    Raises:
        ValueError: If input parameters are invalid
        TypeError: If column type is not string
        RuntimeError: If index building fails or pylance version is incompatible
    """
    # Check pylance version compatibility
    try:
        lance_version = version.parse(lance.__version__)
        min_required_version = version.parse("0.36.0")

        if lance_version < min_required_version:
            raise RuntimeError(
                f"Distributed indexing requires pylance >= 0.36.0, but found {lance.__version__}. "
                "The distribute-related interfaces are not available in older versions. "
                "Please upgrade pylance by running: pip install --upgrade pylance"
            )

        logger.info(f"Pylance version check passed: {lance.__version__} >= 0.36.0")

    except AttributeError as err:
        # If lance.__version__ doesn't exist, assume it's too old
        raise RuntimeError(
            "Cannot determine pylance version. Distributed indexing requires pylance >= 0.36.0. "
            "Please upgrade pylance by running: pip install --upgrade pylance"
        ) from err

    index_id = str(uuid.uuid4())
    logger.info(f"Starting distributed index build with ID: {index_id}")

    # Basic input validation
    if not column:
        raise ValueError("Column name cannot be empty")

    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")

    if index_type not in ["INVERTED", "FTS"]:
        raise ValueError(f"Index type must be 'INVERTED' or 'FTS', not '{index_type}'")

    # Note: Ray initialization is now handled by the Pool, following the pattern from io.py
    # This removes the need for explicit ray.init() calls

    # Load dataset
    if isinstance(dataset, str):
        dataset_uri = dataset
        dataset = LanceDataset(dataset_uri, storage_options=storage_options)
    else:
        dataset_uri = dataset.uri

    # Validate column exists and has correct type
    try:
        field = dataset.schema.field(column)
    except KeyError as e:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(f"Column '{column}' not found. Available: {available_columns}") from e

    if storage_options is None:
        storage_options = dataset._storage_options

    # Check column type
    value_type = field.type
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_type = field.type.value_type

    if not pa.types.is_string(value_type) and not pa.types.is_large_string(value_type):
        raise TypeError(f"Column {column} must be string type, got {value_type}")

    if name is None:
        name = generate_default_index_name(column, index_type, dataset)

    # Get fragments
    fragments = dataset.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")

    fragment_ids = [fragment.fragment_id for fragment in fragments]

    # Adjust num_workers if needed
    if num_workers > len(fragment_ids):
        num_workers = len(fragment_ids)
        logger.info(f"Adjusted num_workers to {num_workers} to match fragment count")

    # Distribute fragments to workers using balanced distribution algorithm
    fragment_batches = _distribute_fragments_balanced(
        fragments, num_workers, logger
    )

    # Phase 1: Fragment index creation using Pool pattern (similar to io.py)
    # Use Pool to distribute work instead of direct Ray task submission
    pool = Pool(processes=num_workers, ray_remote_args=ray_remote_args)

    # Create the fragment handler function
    fragment_handler = _handle_fragment_index(
        dataset_uri=dataset_uri,
        column=column,
        index_type=index_type,
        name=name,
        fragment_uuid=index_id,
        storage_options=storage_options,
        **kwargs,
    )

    # Submit tasks using Pool.map_async
    rst_futures = pool.map_async(
        fragment_handler,
        fragment_batches,
        chunksize=1,
    )

    # Wait for results
    try:
        results = rst_futures.get()
    except Exception as e:
        pool.close()
        raise RuntimeError(f"Failed to complete distributed index building: {e}") from e
    finally:
        pool.close()

    # Check for failures
    failed_results = [r for r in results if r["status"] == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results]
        raise RuntimeError(f"Index building failed: {'; '.join(error_messages)}")

    # Reload dataset to get the latest state after fragment index creation
    dataset = LanceDataset(dataset_uri, storage_options=storage_options)

    # Phase 2: Merge index metadata using the distributed API
    logger.info(f"Phase 2: Merging index metadata for index ID: {index_id}")
    merge_index_metadata_compat(dataset, index_id)

    # Phase 3: Create Index object and commit the operation
    logger.info(f"Phase 3: Creating and committing index '{name}'")

    # Get field information from successful results
    successful_results = [r for r in results if r["status"] == "success"]
    if not successful_results:
        raise RuntimeError("No successful index creation results found")

    fields = successful_results[0]["fields"]

    # Create Index object
    index = Index(
        uuid=index_id,
        name=name,
        fields=fields,
        dataset_version=dataset.version,
        fragment_ids=set(fragment_ids),
        index_version=0,
    )

    # Create and commit the index operation
    create_index_op = lance.LanceOperation.CreateIndex(
        new_indices=[index],
        removed_indices=[],
    )

    updated_dataset = lance.LanceDataset.commit(
        dataset_uri,
        create_index_op,
        read_version=dataset.version,
        storage_options=storage_options,
    )

    logger.info(f"Successfully created distributed index '{name}' with three-phase workflow")
    logger.info(f"Index ID: {index_id}, Fragments: {len(fragment_ids)}, Workers: {len(fragment_batches)}")
    return updated_dataset
