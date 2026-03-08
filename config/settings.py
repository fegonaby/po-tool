import os


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "main")
DATABRICKS_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "default")


def qualified_table(name: str) -> str:
    """Return fully qualified Databricks table name: catalog.schema.table."""
    return f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{name}"
