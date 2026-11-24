import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "glue_jobs"))
sys.path.insert(0, str(ROOT / "orchestration"))


@pytest.fixture(scope="session")
def spark():
    from common.runtime import local_spark

    session = local_spark("tests")
    yield session
    session.stop()
