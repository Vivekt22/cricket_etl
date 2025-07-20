
"""
Unit tests for innings_bronze module.

Covers:
- flatten_struct: unpacks struct columns into top-level fields
- extract_innings: transforms a single inning's dictionary into a Polars DataFrame
- write_innings: verifies that innings data is saved correctly
"""

import polars as pl
from unittest import mock

from cricket_etl.pipeline.p03_bronze.innings_bronze import (
    flatten_struct, extract_innings, write_innings
)
from cricket_etl.helpers.catalog import Catalog


def test_flatten_struct_basic():
    """Test flatten_struct with a simple struct column and no renaming."""
    df = pl.DataFrame({
        "meta": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    })

    flattened = flatten_struct(df, "meta")
    assert "a" in flattened.columns
    assert "b" in flattened.columns
    assert "meta" not in flattened.columns
    assert flattened.shape == (2, 2)


def test_flatten_struct_with_rename():
    """Test flatten_struct with field renaming and conflict resolution."""
    df = pl.DataFrame({
        "meta": [{"x": 10, "y": 20}]
    })

    # Rename 'x' to 'z', but 'z' already exists so it should rename to 'z_1'
    df = df.with_columns(pl.lit("existing").alias("z"))
    flattened = flatten_struct(df, "meta", rename_map={"x": "z"})

    assert "z_1" in flattened.columns
    assert "y" in flattened.columns
    assert "z" in flattened.columns  # original column
    assert flattened["z_1"][0] == 10


def test_extract_innings_basic_structure():
    """Test that extract_innings flattens deliveries and appends metadata."""
    inning = {
        "team": "Team A",
        "overs": [
            {"over": 1, "deliveries": [{"runs": {"batter": 4, "total": 4}}]},
            {"over": 2, "deliveries": [{"runs": {"batter": 1, "total": 1}}]}
        ]
    }

    df = extract_innings("match_123", inning, 0)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 2
    assert "match_id" in df.columns
    assert "inning_num" in df.columns
    assert "batting_team" in df.columns
    assert "ball" in df.columns
    assert df["batting_team"][0] == "Team A"
    assert df["total_runs"].sum() == 5


def test_extract_innings_missing_overs():
    """Test that extract_innings gracefully handles innings without overs."""
    inning = {"team": "Team B"}  # no "overs" key
    result = extract_innings("match_456", inning, 1)
    assert result is None


def test_write_innings(tmp_path):
    """Test that write_innings creates the expected Parquet file."""
    df = pl.DataFrame({
        "match_id": ["m1"],
        "inning_num": [1],
        "batting_team": ["Team X"],
        "ball": [1]
    })

    mock_catalog = mock.Mock(spec=Catalog)
    mock_catalog.bronze.innings_bronze = tmp_path / "innings.parquet"

    write_innings(df, mock_catalog)

    assert mock_catalog.bronze.innings_bronze.exists()
