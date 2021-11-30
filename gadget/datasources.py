import os
from abc import ABC, abstractmethod
from typing import Optional

# TODO
# ds.isDeltaTable
# ds.readDelta
# ds.readParquet
# ...

class DataSource(ABC):
    def __init__(self, source_type):
        self.source_type = source_type


def compose_path(left: str, right: str) -> str:
    return os.path.join(left, *right.split("/"))


class FileDataSource(DataSource):
    def __init__(self, source_type, path_prefix: Optional[str] = None):
        super().__init__(source_type)
        self.path_prefix = path_prefix

    @abstractmethod
    def root_path(self):
        pass

    def path(self, to: str) -> str:
        return compose_path(self.root_path(), to)

    def __str__(self) -> str:
        return f"[{self.source_type}] {self.root_path()}"


class DataSourceLocal(FileDataSource):
    def __init__(self, path_prefix: Optional[str] = None):
        super().__init__("Local", path_prefix=path_prefix)
        self.path_prefix = path_prefix

    @property
    def root_path(self):
        return self.path_prefix if self.path_prefix is not None else "./"


class DataSourceDbfs(FileDataSource):
    def __init__(self, path_prefix: Optional[str] = None):
        super().__init__("DBFS", path_prefix=path_prefix)

        self.protocol = "dbfs"
        self._path = f"{self.protocol}:/FileStore"
        self.path_prefix = path_prefix

    @property
    def root_path(self):
        return (
            compose_path(self._path, self.path_prefix)
            if self.path_prefix is not None
            else self._path
        )


class DataSourceAzBlob(FileDataSource):
    def __init__(self, blob: str, container: str, path_prefix: Optional[str] = None):
        super().__init__("Azure Blob", path_prefix=path_prefix)

        self.protocol = "wasbs"
        self._path = f"{self.protocol}://{container}@{blob}.blob.core.windows.net"
        self.blob = blob
        self.container = container
        self.path_prefix = path_prefix

    @property
    def root_path(self):
        return (
            compose_path(self._path, self.path_prefix)
            if self.path_prefix is not None
            else self._path
        )


class DataSourceAdls2(FileDataSource):
    def __init__(self, blob: str, container: str, path_prefix: Optional[str] = None):
        super().__init__("ADLS Gen2", path_prefix=path_prefix)

        self.protocol = "abfss"
        self._path = f"{self.protocol}://{container}@{blob}.dfs.core.windows.net"
        self.blob = blob
        self.container = container
        self.path_prefix = path_prefix

    @property
    def root_path(self):
        return (
            compose_path(self._path, self.path_prefix)
            if self.path_prefix is not None
            else self._path
        )
