import os 

# Environment Configuration
global_ENV = os.getenv("ENVIRONMENT", "Missing ENV Config").lower()
global_SANDBOXNAME = os.getenv("SANDBOXNAME", "NA").upper()

# Unity Catalog Configuration
global_UNITYCATALOG_CONTAINERNAME = 'test-unity'
global_CATALOGNAME = f'metastore_{global_ENV.lower()}'

# Base Storage Paths for Data Lake
global_BASEMOUNTPATH = 'abfss://{}@teststorage{}.dfs.core.windows.net'

# Delta Lake Storage Paths (Bronze, Silver, Gold, Stage)
global_STAGE_BASE_DELTA_PATH = global_BASEMOUNTPATH.format('stage', global_ENV.lower()) + '/delta/'
global_BRONZE_BASE_DELTA_PATH = global_BASEMOUNTPATH.format('bronze', global_ENV) + '/delta'
global_SILVER_BASE_DELTA_PATH = global_BASEMOUNTPATH.format('silver', global_ENV) + '/delta'
global_GOLD_BASE_DELTA_PATH = global_BASEMOUNTPATH.format('gold', global_ENV) + '/delta'

def get_catalog_name():
    """Returns the catalog name based on the environment or sandbox setting."""
    return f'metastore_{global_SANDBOXNAME.lower()}' if global_SANDBOXNAME.upper() != 'NA' else global_CATALOGNAME
