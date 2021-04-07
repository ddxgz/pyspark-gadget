from pyspark.sql import DataFrame, Column, functions as F
from databricks.dbutils_v1.DBUtilsHolder import dbutils


def connectionString(storageAccount: str,
                     secretScope: str,
                     storageAccountKeyName: str) -> str:

    storageKey = dbutils.secrets.get(
        scope=secretScope, key=storageAccountKeyName)

    return f"DefaultEndpointsProtocol=https;AccountName={storageAccount};AccountKey={storageKey};EndpointSuffix=core.windows.net"


class DataSourceAdls2:

    def __init__(self,
                 blob: str,
                 container: str,
                 secretScope: str,
                 secretKey: str,
                 pathPrefix=None,
                 ):

        self.storageKeySource = dbutils.secrets.get(
            scope=secretScope, key=secretKey)

        abfssRootPath = f"abfss://{container}@{blob}.dfs.core.windows.net"

        self.rootPath = f"{abfssRootPath}/{pathPrefix}" if pathPrefix else abfssRootPath

        self.connString: str = connectionString(blob, secretScope, secretKey)

        spark.conf.set(
            f"fs.azure.account.key.{blob}.dfs.core.windows.net",
            self.storageKeySource
        )

    def __str__(self):
        return self.rootPath


def getJdbcUrl(name: str,
               port: int,  # 1433
               database: str,
               secretScope: str,
               userKey: str,
               passwordKey: str) -> str:

    dw = name
    jdbcPort = port
    hostname = f"{dw}.database.windows.net"
    user = dbutils.secrets.get(scope=secretScope, key=userKey)
    pw = dbutils.secrets.get(scope=secretScope, key=passwordKey)

    return f"jdbc:sqlserver://{hostname}:{jdbcPort};database={database};user={user}@{dw};password={pw};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


def confBlobKey(blob: str,
                container: str,
                secretScope: str,
                secretKey: str = None):
    storageKeySource = dbutils.secrets.get(
        scope=secretScope, key=secretKey)

    spark.conf.set(
        f"fs.azure.account.key.{blob}.blob.core.windows.net",
        storageKeySource
    )


def loadDwTable(tableName: str, pathTempDir: str) -> DataFrame:
    spark.read \
        .format("com.databricks.spark.sqldw") \
        .option("url", getJdbcUrl()) \
        .option("tempDir", pathTempDir) \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("dbTable", tableName) \
        .load()


def mountAnyway(
        mountSource: str,
        mountPath: str,
        storageAccount: str,
        storageKey: str):

    mounted = [x for x in dbutils.fs.mounts() if x.mountPoint == mountPath]
    msg = f"{mountPath}"

    if len(mounted) == 1 and mounted[0].mountPoint == mountPath:
        if mounted[0].source == mountSource:
            return msg + f" already mounted source {mountSource}"

        msg += f"already mounted a different source {mounted[0].source}, will unmount and remount the new source"
        dbutils.fs.unmount(mountPath)

    dbutils.fs.mount(
        source=mountSource,
        mount_point=mountPath,
        extra_configs={
            f"fs.azure.account.key.{storageAccount}.blob.core.windows.net": storageKey
        }
    )

    msg += f"\nMounted {mountPath} to source {mountSource}"
    return msg


class DataSource:

    def __init__(self,
                 blob: str,
                 container: str,
                 secretKey: str,
                 secretScope: str,
                 blobSecret=None,
                 pathPrefix=None,
                 mountNow: bool = True):
        self.blob = blob
        self.container = container

        self.storageKeySource = dbutils.secrets.get(
            scope=secretScope, key=secretKey)

        self.mountSource = f"wasbs://{container}@{blob}.blob.core.windows.net"

        self.mountPath = f"/mnt/{blob}-{container}"

        self.rootPath = f"dbfs://{self.mountPath}/{pathPrefix}" if pathPrefix else f"dbfs://{self.mountPath}/"

        self.connString: str = connectionString(blob, secretScope, secretKey)

        if mountNow:
            self.mount()

    def __str__(self):
        return self.rootPath

    def mount(self) -> str:
        mountAnyway(self.mountSource,
                    self.mountPath,
                    self.blob,
                    self.storageKeySource)


def DataTarget(blobSecret: str,
               container: str,
               secretKey: str,
               secretScope: str,
               pathPrefix=None,
               mountNow: bool = True) -> DataSource:

    blob = dbutils.secrets.get(scope=secretScope, key=blobSecret)

    return DataSource(
        blob,
        container,
        secretKey=secretKey,
        secretScope=secretScope,
        pathPrefix=pathPrefix,
        mountNow=mountNow
    )
