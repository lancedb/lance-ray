from pathlib import Path

import lance_ray as lr


class TestScanner:
    def test_scan_partial_columns(self, sample_dataset, temp_dir):
        path = Path(temp_dir) / "scanner.lance"
        lr.write_lance(sample_dataset, path)
        ds = lr.scanner(path).columns(["id", "name"]).to_scanner().to_dataset()
        assert ds is not None
        assert ds.take(1) == [{"id": 1, "name": "Alice"}]

    def test_scan_concurrency(self, sample_dataset, temp_dir):
        path = Path(temp_dir) / "scanner.lance"
        lr.write_lance(sample_dataset, path, max_rows_per_file=3)
        ds = lr.scanner(path, concurrency=2).to_scanner().to_dataset()
        assert ds is not None
        assert ds.count() == 5
        assert ds.sort("id").take(1) == [{"id": 1, "name": "Alice", "age": 25, "score": 85.5}]
