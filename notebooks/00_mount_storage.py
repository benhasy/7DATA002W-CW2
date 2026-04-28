# Configure direct ADLS Gen2 access using storage account key
STORAGE_ACCOUNT = "stcw2bdcbh"
STORAGE_KEY = dbutils.secrets.get(scope="kv-scope", key="storage-account-key")

# Set Spark config for direct access
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# Test connection by listing the raw container
files = dbutils.fs.ls(f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/")
for f in files:
    print(f.path)

print("Storage connection successful.")