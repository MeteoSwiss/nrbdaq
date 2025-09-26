# /tests/tests_integration.py
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import polars as pl

# Project imports
from nrbdaq.utils.utils import load_config
from nrbdaq.instr.ae31 import AE31


# -------------------------
# Integration fixtures
# -------------------------

@pytest.fixture(scope="session")
def config() -> dict[str, Any]:
    """Loads the real nrbdaq.yml; relies on your environment -> integration."""
    return load_config(config_file="nrbdaq.yml")


@pytest.fixture
def ae31_instance(config: dict[str, Any]) -> AE31:
    """Real AE31 instance; exercises the actual csv_to_df pipeline."""
    return AE31(config=config)


# -------------------------
# Integration tests
# -------------------------

def test_config_host(config: dict[str, Any]):
    # Reads the actual nrbdaq.yml (environment-dependent)
    assert config["sftp"]["host"] == "sftp.meteoswiss.ch"


def test_validate_ae31_csv_file_schemas(ae31_instance: AE31):
    """
    Uses real AE31 CSVs if available.
    Skips gracefully if files are absent so CI isn't brittle.
    """
    data_dir = Path("tests/data/ae31")
    valid_file = data_dir / "AE31_20240825.csv"
    test_file = data_dir / "AE31_20240805.csv"

    if not (valid_file.exists() and test_file.exists()):
        pytest.skip("AE31 test CSV files not found; skipping schema comparison test.")

    df_valid = ae31_instance.csv_to_df(file=str(valid_file))
    df_test = ae31_instance.csv_to_df(file=str(test_file))

    assert isinstance(df_valid, pl.DataFrame)
    assert isinstance(df_test, pl.DataFrame)
    assert df_valid.schema == df_test.schema
