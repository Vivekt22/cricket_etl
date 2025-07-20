
import pytest
import zipfile
import io
from unittest import mock

from cricket_etl.helpers.catalog import Catalog
from cricket_etl.pipeline.p01_ingest.ingest import (
    download_zip, empty_ingest_folder, extract_jsons_from_zip
)

@pytest.fixture
def mock_catalog(tmp_path):
    # Setup fake catalog with necessary paths
    zip_file = tmp_path / "all_json.zip"
    ingest_folder = tmp_path / "ingest"
    ingest_folder.mkdir()

    return mock.Mock(spec=Catalog, **{
        "cricsheet_url": "http://fake-url.com/data.zip",
        "all_json_zip": zip_file,
        "ingest": ingest_folder
    })

@mock.patch("cricket_etl.pipeline.p01_ingest.ingest.requests.get")
def test_download_zip(mock_get, mock_catalog):
    # Mock the HTTP response
    mock_response = mock.Mock()
    mock_response.content = b"Fake zip content"
    mock_response.raise_for_status = mock.Mock()
    mock_get.return_value = mock_response

    download_zip(mock_catalog)

    # Assert file is created with expected content
    assert mock_catalog.all_json_zip.exists()
    assert mock_catalog.all_json_zip.read_bytes() == b"Fake zip content"
    mock_response.raise_for_status.assert_called_once()

def test_empty_ingest_folder(mock_catalog):
    # Create dummy files and folders
    file1 = mock_catalog.ingest / "file1.json"
    file1.write_text("data")
    subdir = mock_catalog.ingest / "subdir"
    subdir.mkdir()

    # Add file in subdir (simulate illegal deletion)
    (subdir / "file2.json").write_text("data")

    # Should raise an error due to non-empty dir
    with pytest.raises(OSError):
        empty_ingest_folder(mock_catalog)

    # Remove subdir file and retry
    (subdir / "file2.json").unlink()
    empty_ingest_folder(mock_catalog)

    assert list(mock_catalog.ingest.iterdir()) == []

def test_extract_jsons_from_zip(mock_catalog):
    # Create a fake zip with json and non-json files
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, 'w') as zf:
        zf.writestr("match1.json", '{"match":1}')
        zf.writestr("readme.txt", "This is a readme")

    zip_bytes.seek(0)
    mock_catalog.all_json_zip.write_bytes(zip_bytes.read())

    extract_jsons_from_zip(mock_catalog)

    extracted_files = list(mock_catalog.ingest.glob("*.json"))
    assert len(extracted_files) == 1
    assert extracted_files[0].name == "match1.json"
