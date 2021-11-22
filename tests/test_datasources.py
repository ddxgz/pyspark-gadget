import pytest

from gadget.datasources import DataSourceLocal, DataSourceAdls2


def test_datasourcelocal():
    ds1 = DataSourceLocal(path_prefix="./")
    ds2 = DataSourceLocal()

    assert ds1.root_path == ds2.root_path

    assert ds1.source_type == "Local"


def test_datasourceadls2():
    blob = "blob"
    container = "container"

    ds = DataSourceAdls2(
        blob=blob,
        container=container,
        path_prefix="/path/to",
    )

    assert ds.root_path == ds._path + "/path/to"


    ds2 = DataSourceAdls2(
        blob=blob,
        container=container,
        path_prefix="pathto",
    )

    assert ds2.root_path == ds2._path + "/pathto"