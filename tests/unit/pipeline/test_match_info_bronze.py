"""
Unit tests for match_info_bronze module.

These tests validate the extraction, transformation, and saving
of match-level metadata from cricket match JSON data.

Modules tested:
- extract_match_info
- get_match_info_df
- write_match_info
"""

import pytest
import polars as pl
from unittest import mock

from cricket_etl.pipeline.p03_bronze.match_info_bronze import (
    extract_match_info, get_match_info_df, write_match_info
)
from cricket_etl.helpers.catalog import Catalog

# -------------------------------
# Fixture to simulate match info
# -------------------------------
@pytest.fixture
def sample_match_info_dict():
    """Returns a sample nested dictionary simulating real match info JSON structure."""
    return {
        "balls_per_over": 6,
        "city": "Delhi",
        "dates": ["2022-04-01", "2022-04-02"],
        "event": {"name": "IPL 2022"},
        "gender": "male",
        "match_type": "T20",
        "officials": {
            "match_referees": ["Ref A"],
            "umpires": ["Ump 1", "Ump 2"]
        },
        "outcome": {
            "winner": "Team A",
            "by": {"runs": 25},
            "result": "normal"
        },
        "overs": 20,
        "player_of_match": ["Player X"],
        "season": "2022/23",
        "team_type": "domestic",
        "teams": ["Team A", "Team B"],
        "toss": {"winner": "Team B", "decision": "bat"},
        "venue": "Stadium 1"
    }

# ---------------------------------------------------------
# Test extract_match_info: parse dictionary into flat dict
# ---------------------------------------------------------
def test_extract_match_info(sample_match_info_dict):
    """Test that extract_match_info correctly flattens nested match info into a flat dict."""
    match_id = "match_001"
    result = extract_match_info(match_id, sample_match_info_dict)

    assert result["match_id"] == "match_001"
    assert result["balls_per_over"] == 6
    assert result["start_date"] == "2022-04-01"
    assert result["end_date"] == "2022-04-02"
    assert result["match_referee"] == "Ref A"
    assert result["win_by"] == "runs"
    assert result["win_margin"] == 25
    assert result["season"] == "2022"  # year extracted from "2022/23"
    assert result["team_01"] == "Team A"
    assert result["team_02"] == "Team B"

# ----------------------------------------------------------------------
# Test get_match_info_df: simulate reading from pickle and return DataFrame
# ----------------------------------------------------------------------
@mock.patch("cricket_etl.helpers.functions.Functions.read_pickle")
def test_get_match_info_df(mock_read_pickle, tmp_path):
    """Test get_match_info_df using a fake .pkl file and patched read_pickle function."""

    # Create a mock Catalog object with a temporary path
    mock_catalog = mock.Mock(spec=Catalog)
    fake_file = tmp_path / "match_001.pkl"
    fake_file.touch()  # create empty file

    mock_catalog.raw.glob.return_value = [fake_file]

    # Mock the content that read_pickle should return
    mock_read_pickle.return_value = {"info": {
        "balls_per_over": 6,
        "dates": ["2022-04-01"],
        "season": "2022/23"
    }}

    # Run the function
    df = get_match_info_df(mock_catalog)

    # Assert correctness of resulting Polars DataFrame
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 1
    assert "balls_per_over" in df.columns
    assert df["season"][0] == 2022  # casted to integer

# ----------------------------------------------------------
# Test write_match_info: ensure file is saved successfully
# ----------------------------------------------------------
def test_write_match_info(tmp_path):
    """Test that write_match_info saves the DataFrame as a Parquet file."""

    # Create a minimal DataFrame with required columns
    df = pl.DataFrame({
        "match_id": ["m1"],
        "balls_per_over": [6],
        "start_date": ["2022-04-01"],
        "end_date": ["2022-04-02"],
        "win_margin": [25],
        "default_overs": [20],
        "season": [2022]
    })

    # Create a fake Catalog with a bronze path pointing to tmp
    mock_catalog = mock.Mock(spec=Catalog)
    mock_catalog.bronze.match_info_bronze = tmp_path / "match_info.parquet"

    # Write the DataFrame and validate that the file exists
    write_match_info(df, mock_catalog)
    assert mock_catalog.bronze.match_info_bronze.exists()