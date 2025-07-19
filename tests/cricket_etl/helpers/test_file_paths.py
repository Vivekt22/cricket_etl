from cricket_etl.helpers.catalog import ROOT_PATH, DATA_PATH, BaseDataPaths

def test_enum_members_are_paths():
    for folder in BaseDataPaths:
        assert folder.value.name.startswith("0"), f"{folder} does not follow naming convention"
        assert folder.value.is_relative_to(folder.value.parent), "Enum member is not a path"

def test_paths_under_data_path():
    for folder in BaseDataPaths:
        assert str(folder.value).startswith(str(DATA_PATH)), f"{folder} is not under data/"

def test_directories_exist():
    for folder in BaseDataPaths:
        assert folder.value.exists(), f"{folder.value} does not exist"