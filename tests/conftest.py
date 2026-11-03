import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from warehouse.schema import init_schema  # noqa: E402


@pytest.fixture()
def conn(tmp_path):
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    init_schema(conn)
    yield conn
    conn.close()
