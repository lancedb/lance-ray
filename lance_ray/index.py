# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import logging
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Union

import pyarrow as pa
import ray

import lance
from lance.dataset import Index, LanceOperation, LanceDataset

logger = logging.getLogger(__name__)


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


@ray.remote
def _build_fragment_index_task(
    dataset_uri: str,
    column: str,
    fragment_ids: List[int],
    index_type: str = "INVERTED",
    name: Optional[str] = None,
    fragment_uuid: Optional[str] = None,
    storage_options: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Ray remote task for building fragment index using the distributed API.

    This task calls create_scalar_index directly for specific fragments.
    After execution, fragment-level indices are automatically built.

    Args:
        dataset_uri: URI of the Lance dataset
        column: Column name to index
        fragment_ids: List of fragment IDs to build index for
        index_type: Type of index to build ("INVERTED" or "FTS")
        name: Name of the index
        fragment_uuid: UUID for the fragment index
        storage_options: Storage options for the dataset
        **kwargs: Additional arguments to pass to create_scalar_index

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

        if name is None:
            name = generate_default_index_name(column, index_type, dataset)

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


def create_fts_index(
    dataset: Union[str, "lance.LanceDataset"],
    column: str,
    index_type: str = "INVERTED",
    name: Optional[str] = None,
    num_workers: int = 4,
    storage_options: Optional[Dict[str, str]] = None,
    ray_options: Optional[Dict[str, Any]] = None,
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
        ray_options: Options for Ray tasks (e.g., num_cpus, resources)
        **kwargs: Additional arguments to pass to create_scalar_index

    Returns:
        Updated Lance dataset with the index created

    Raises:
        ValueError: If input parameters are invalid
        TypeError: If column type is not string
        RuntimeError: If index building fails
    """
    index_id = str(uuid.uuid4())
    logger.info(f"Starting distributed index build with ID: {index_id}")

    # Basic input validation
    if not column:
        raise ValueError("Column name cannot be empty")

    if num_workers <= 0:
        raise ValueError(f"num_workers must be positive, got {num_workers}")

    if index_type not in ["INVERTED", "FTS"]:
        raise ValueError(f"Index type must be 'INVERTED' or 'FTS', not '{index_type}'")

    # Initialize Ray if needed
    if not ray.is_initialized():
        try:
            if "RAY_ADDRESS" in os.environ:
                # Connect to existing Ray cluster
                ray.init(
                    address=os.environ["RAY_ADDRESS"],
                    ignore_reinit_error=True,
                    runtime_env={
                        "env_vars": {
                            "TOSFS_LOGGING_LEVEL": "INFO",
                            "LANCE_LOG": "DEBUG",
                        }
                    },
                )
            else:
                # Start local Ray cluster
                ray.init(
                    ignore_reinit_error=True,
                    runtime_env={
                        "env_vars": {
                            "TOSFS_LOGGING_LEVEL": "INFO",
                            "LANCE_LOG": "DEBUG",
                        }
                    },
                )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Ray: {e}")

    # Load dataset
    if isinstance(dataset, str):
        dataset_uri = dataset
        dataset = LanceDataset(dataset_uri, storage_options=storage_options)
    else:
        dataset_uri = dataset.uri

    # Validate column exists and has correct type
    try:
        field = dataset.schema.field(column)
    except KeyError:
        available_columns = [field.name for field in dataset.schema]
        raise ValueError(f"Column '{column}' not found. Available: {available_columns}")

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

    # Distribute fragments to workers
    fragment_batches = []
    batch_size = max(1, len(fragment_ids) // num_workers)

    for i in range(0, len(fragment_ids), batch_size):
        batch = fragment_ids[i : i + batch_size]
        if batch:
            fragment_batches.append(batch)

    # Basic Ray task options
    task_options = {"num_cpus": 1}
    if ray_options:
        task_options.update(ray_options)

    # Phase 1: Fragment index creation (completed by Ray tasks)
    # Each Ray task calls create_scalar_index for its assigned fragments
    # After task completion, fragment-level indices are automatically built
    
    # Submit tasks
    tasks = []
    for batch in fragment_batches:
        task = _build_fragment_index_task.options(**task_options).remote(
            dataset_uri=dataset_uri,
            column=column,
            fragment_ids=batch,
            index_type=index_type,
            name=name,
            fragment_uuid=index_id,
            storage_options=storage_options,
            **kwargs,
        )
        tasks.append(task)

    # Wait for results
    try:
        results = ray.get(tasks)
    except Exception as e:
        raise RuntimeError(f"Failed to complete distributed index building: {e}")

    # Check for failures
    failed_results = [r for r in results if r["status"] == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results]
        raise RuntimeError(f"Index building failed: {'; '.join(error_messages)}")

    # Reload dataset to get the latest state after fragment index creation
    dataset = LanceDataset(dataset_uri, storage_options=storage_options)

    # Phase 2: Merge index metadata using the distributed API
    logger.info(f"Phase 2: Merging index metadata for index ID: {index_id}")
    dataset.merge_index_metadata(index_id)
    
    # Phase 3: Create Index object and commit the operation
    logger.info(f"Phase 3: Creating and committing index '{name}'")
    from lance.dataset import Index
    
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
