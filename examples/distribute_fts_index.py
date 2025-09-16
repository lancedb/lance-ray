#!/usr/bin/env python3
"""
Demonstration of distributed text indexing with PR #4578 enhancements.
This example shows how to use the enhanced distributed text indexing functionality
that leverages the new API

Requirements:
- ray
- lance_ray
- lance (with PR #4578 changes)
"""

import logging
import tempfile
from pathlib import Path

import lance
import lance_ray as lr
import pandas as pd
import ray

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_sample_dataset(num_fragments=4, rows_per_fragment=1000):
    """Generate a sample dataset with multiple fragments for testing."""
    logger.info(f"Generating dataset with {num_fragments} fragments, {rows_per_fragment} rows each")

    all_data = []
    for frag_idx in range(num_fragments):
        for row_idx in range(rows_per_fragment):
            row_id = frag_idx * rows_per_fragment + row_idx
            all_data.append({
                "id": row_id,
                "title": f"Document {row_id}: Sample Title for Fragment {frag_idx}",
                "content": f"This is the content of document {row_id}. "
                          f"It contains sample text for testing distributed indexing functionality. "
                          f"Fragment ID: {frag_idx}. Keywords: distributed, indexing, lance, ray, search.",
                "category": ["technology", "database", "search", "distributed"][frag_idx % 4],
                "fragment_id": frag_idx,
            })

    return pd.DataFrame(all_data)


def create_multi_fragment_dataset(data_df, output_path, max_rows_per_file):
    """Create a Lance dataset with multiple fragments."""
    logger.info(f"Creating Lance dataset at {output_path}")

    # Convert to Ray dataset
    ray_dataset = ray.data.from_pandas(data_df)

    # Write as Lance dataset with multiple fragments
    lr.write_lance(ray_dataset, output_path, max_rows_per_file=max_rows_per_file)

    # Load and return Lance dataset
    dataset = lance.dataset(output_path)
    logger.info(f"Created dataset with {len(dataset.get_fragments())} fragments")

    return dataset


def demonstrate_new_api_features(dataset, column="content"):
    """Demonstrate the new distributed indexing API features from PR #4578."""
    logger.info("=== Demonstrating New API Features (PR #4578) ===")

    try:
        # Build distributed index using the enhanced API
        logger.info("Building distributed index with new API features...")

        updated_dataset = lr.create_scalar_index(
            dataset=dataset,
            column=column,
            index_type="INVERTED",
            name="pr4578_enhanced_idx",
            num_workers=4,
            remove_stop_words=False,
            with_position=True,  # Enable phrase queries
        )

        logger.info("✅ Successfully created distributed index with new API")

        # Verify index creation
        indices = updated_dataset.list_indices()
        logger.info(f"Total indices: {len(indices)}")

        for idx in indices:
            if idx["name"] == "pr4578_enhanced_idx":
                logger.info(f"✅ Found our index: {idx['name']} (type: {idx['type']})")
                break
        else:
            logger.warning("❌ Our index not found in the list")

        return updated_dataset

    except Exception as e:
        logger.error(f"❌ Error with new API: {e}")
        logger.info("This is expected if PR #4578 changes are not available")
        return None


def demonstrate_search_functionality(dataset, search_queries=None):
    """Demonstrate search functionality with the created index."""
    logger.info("=== Demonstrating Search Functionality ===")

    if search_queries is None:
        search_queries = [
            "distributed indexing",
            "lance ray",
            "sample text",
            "technology database",
        ]

    for query in search_queries:
        try:
            logger.info(f"Searching for: '{query}'")

            results = dataset.scanner(
                full_text_query=query,
                columns=["id", "title", "content", "category"],
            ).to_table()

            logger.info(f"  Found {results.num_rows} results")

            if results.num_rows > 0:
                # Show top 3 results
                for i in range(min(3, results.num_rows)):
                    title = results.column("title")[i].as_py()
                    category = results.column("category")[i].as_py()
                    logger.info(f"  [{i+1}] {title} (Category: {category})")

        except Exception as e:
            logger.error(f"❌ Search failed for '{query}': {e}")





def main():
    """Main demonstration function."""
    logger.info("🚀 Starting Distributed Text Indexing Demo (PR #4578)")

    # Initialize Ray
    if not ray.is_initialized():
        ray.init(local_mode=False, ignore_reinit_error=True)
        logger.info("✅ Ray initialized")

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Generate sample data
            data_df = generate_sample_dataset(num_fragments=4, rows_per_fragment=500)

            # Create multi-fragment dataset
            dataset_path = Path(temp_dir) / "pr4578_demo_dataset.lance"
            dataset = create_multi_fragment_dataset(
                data_df, str(dataset_path), max_rows_per_file=500
            )

            logger.info(f"Dataset created with {len(dataset.get_fragments())} fragments")

            # Demonstrate new API features
            enhanced_dataset = demonstrate_new_api_features(dataset)

            if enhanced_dataset:
                # Test search functionality with new API
                demonstrate_search_functionality(enhanced_dataset)
                logger.info("🎉 Demo completed successfully!")
            else:
                logger.error("❌ New API features not available - PR #4578 may not be merged")

    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        raise

    finally:
        # Clean up Ray
        if ray.is_initialized():
            ray.shutdown()
            logger.info("✅ Ray shutdown")


if __name__ == "__main__":
    main()
