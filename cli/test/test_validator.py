import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from graphar_cli.checker import get_parquet_stats

def create_test_parquet_file(tmp_path: Path):
    table = pa.table({
        "col1": [1, 2, None],
        "col2": ["a", None, "c"],
        "col3": [0.1, None, None]
    })
    file_path = tmp_path / "test.parquet"
    pq.write_table(table, file_path)
    return file_path


def test_get_parquet_stats_basic(tmp_path):
    file_path = create_test_parquet_file(tmp_path)

    columns_map = {
        "col1": "col1_out",
        "col2": "col2_out",
    }
    stats = {}

    get_parquet_stats(str(file_path), columns_map, stats)

    assert stats["col1_out"]["row_count"] == 3
    assert stats["col1_out"]["null_count"] == 1

    assert stats["col2_out"]["row_count"] == 3
    assert stats["col2_out"]["null_count"] == 1


def test_missing_column(tmp_path):
    file_path = create_test_parquet_file(tmp_path)
    columns_map = {
        "nonexistent_col": "out"
    }
    stats = {}

    with pytest.raises(ValueError, match="Columns not found in parquet"):
        get_parquet_stats(str(file_path), columns_map, stats)
