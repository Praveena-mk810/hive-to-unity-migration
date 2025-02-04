from pyspark.sql.utils import AnalysisException
from config import *

def drop_views(schema):
    """ Drops all views in a given schema """
    try:
        views = spark.sql(f"SHOW VIEWS IN hive_metastore.{schema}").collect()
        for view in views:
            full_view_name = f"{schema}.{view['viewName']}"
            spark.sql(f"DROP VIEW IF EXISTS hive_metastore.{full_view_name}")
            print(f"Dropped view: {full_view_name}")
    except Exception as e:
        print(f"Error dropping views in {schema}: {str(e)}")

def update_table_location(schema, table_name, layer='stage'):
    """ Updates table location to new ABFSS path based on layer (Bronze, Silver, Gold, Stage) """
    try:
        tablenamefull = f"hive_metastore.{schema}.{table_name}"
        table_metadata = spark.sql(f"DESCRIBE DETAIL {tablenamefull}").collect()[0]
        
        # Choosing the correct storage path based on layer
        if layer == 'bronze':
            new_path = f"{global_BRONZE_BASE_DELTA_PATH}/{table_name}"
        elif layer == 'silver':
            new_path = f"{global_SILVER_BASE_DELTA_PATH}/{table_name}"
        elif layer == 'gold':
            new_path = f"{global_GOLD_BASE_DELTA_PATH}/{table_name}"
        else:
            new_path = f"{global_STAGE_BASE_DELTA_PATH}/{table_name}"  # Default to Stage for any other layer
        
        spark.sql(f"ALTER TABLE {tablenamefull} SET LOCATION '{new_path}'")
        print(f"Updated location for {table_name} to: {new_path}")
    except Exception as e:
        print(f"Error updating table {table_name}: {str(e)}")

def sync_schema(schema):
    """ Sync schema from Hive Metastore to Unity Catalog """
    try:
        sync_sql = f"SYNC SCHEMA {global_CATALOGNAME}.{schema} AS EXTERNAL FROM hive_metastore.{schema}"
        spark.sql(sync_sql)
        print(f"Synced schema {schema} to Unity Catalog")
    except Exception as e:
        print(f"Error syncing schema {schema}: {str(e)}")

def migrate_schema(schema, layer='stage'):
    """ Runs the full migration process for a schema, with layer-specific storage """
    drop_views(schema)
    tables_df = spark.sql(f"SHOW TABLES IN hive_metastore.{schema}")
    table_names = [row.tableName for row in tables_df.collect()]
    
    for table_name in table_names:
        update_table_location(schema, table_name, layer)
    
    sync_schema(schema)
    print(f"Migration completed for schema: {schema} at layer: {layer}")

if __name__ == "__main__":
    schema = "example_schema"  # Replace with the schema you want to migrate
    layer = "bronze"  # Choose the data layer (stage, bronze, silver, or gold)
    migrate_schema(schema, layer)
    