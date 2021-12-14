import os
from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import DataFrame
from delta.tables import DeltaTable

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
        return compose_path(self.root_path, to)

    def __str__(self) -> str:
        return f"[{self.source_type}] {self.root_path()}"
    
    def ls(self, sub_dir: str=None):
        _dir = compose_path(self.root_path, sub_dir) if sub_dir else self.root_path
        return dbutils.fs.ls(_dir)

        
    def read(self, format: str="delta", options: dict={}):
        return spark.read.format(format).options(**options).load(self.root_path)
    
    def read_delta(self, options: dict={}):
        if not DeltaTable.isDeltaTable(spark, self.root_path):
            print("Not a delta table!! Will return a None")
            return None      
        
        return self.read(format="delta", options=options)
            
    def write(self, df: DataFrame, mode:str, format: str="delta", options: dict={}):
        return df.write.mode(mode).format(format).options(**options).save(self.root_path)
    
    def write_delta(self, df: DataFrame, mode:str, options: dict={}):
        return self.write(df, format="delta", mode=mode, options=options)
    
    def delta_table(self):
        if not DeltaTable.isDeltaTable(spark, self.root_path):
            print("Not a delta table!! Will return a None")
            return None      
        
        return DeltaTable.forPath(spark, self.root_path)
    

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
