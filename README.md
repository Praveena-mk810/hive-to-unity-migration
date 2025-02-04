# Hive to Unity Catalog Migration

This project automates the migration of **external tables** from **Hive Metastore** to **Unity Catalog** in **Databricks**. The focus is on migrating **external tables** using the **SYNC** operation to update schema definitions in Unity Catalog from Hive Metastore. The migration process supports moving data across multiple layers of Delta Lake (Stage, Bronze, Silver, and Gold).

## Project Overview

The **Hive to Unity Catalog Migration** project provides an automated solution for migrating **external tables** and syncing their schemas from **Hive Metastore** to **Unity Catalog**. The current implementation includes the following functionalities:
- Migrating **external tables** from Hive Metastore to Unity Catalog.
- Using **SYNC** to transfer schema definitions from Hive Metastore to Unity Catalog.
- Updating table locations to Delta Lake storage layers (Stage, Bronze, Silver, and Gold).
- Dropping views in Hive Metastore to avoid conflicts during migration.


## Features

- **External Table Migration**: Migrates **external tables** from Hive Metastore to Unity Catalog.
- **Schema Synchronization**: Uses the **SYNC** operation to sync schemas from Hive Metastore to Unity Catalog, specifically for external tables.
- **Table Location Updates**: Updates table locations in Delta Lake across different layers (Stage, Bronze, Silver, Gold).
- **View Management**: Drops views in Hive Metastore to ensure a clean migration process.

## Technologies Used

- **Databricks**: Platform for running Spark-based workloads and managing cloud-based data pipelines.
- **Azure Storage**: For storing Delta Lake tables in various data layers (Stage, Bronze, Silver, Gold).
- **Delta Lake**: A storage layer that enables the management of large-scale data.
- **Python**: Programming language used for scripting the migration logic.
- **PySpark**: Python API for Spark, used to interact with Databricks.
- **Unity Catalog**: A Databricks governance solution for managing, securing, and sharing data.

## Prerequisites

Before running this project, ensure you have the following:

1. **Databricks Workspace**: Access to a Databricks environment with **Hive Metastore** and **Unity Catalog**.
2. **Azure Storage Account**: For storing Delta Lake tables in Stage, Bronze, Silver, and Gold layers.
3. **Python 3.x**: Make sure Python 3.x is installed on your system.
4. **PySpark**: Install PySpark to interact with Databricks.

## Setup Instructions

### 1. Clone the Repository

Clone the repository to your local machine:

### 2. Install Dependencies

Ensure that all required Python packages are installed:


### 3. Configure the `config.py` File

Edit the `config.py` file to configure environment-specific settings:
- Set the **environment** (`global_ENV`) to your target environment (e.g., dev, prod).
- Specify **Delta Lake storage paths** for Stage, Bronze, Silver, and Gold layers.
- Configure your **Unity Catalog** and **Hive Metastore** details.

### 4. Ensure Proper Permissions

Make sure your Databricks workspace has the necessary permissions to interact with both Hive Metastore and Unity Catalog. Ensure your user or service account has **admin permissions** to modify schemas and tables.

## Usage

To run the migration for **external tables**:

1. Ensure that your schema contains **external tables**.
2. Run the migration script, which will:
   - Drop views.
   - Update the table location.
   - Sync the schema using the **SYNC** operation for external tables.


1. **Run the `migration.py` script**:

   The migration process is triggered by running the `migration.py` script, which automatically:
   - Drops views.
   - Updates table locations.
   - Syncs the schema with Unity Catalog.

   ```bash
   python src/migration.py
   ```

2. **Modify the schema name and layer** as needed in the `if __name__ == "__main__":` block of `migration.py`.

   Example:

   ```python
   if __name__ == "__main__":
       schema = "example_schema"  # Replace with the schema you want to migrate
       layer = "bronze"  # Choose the data layer (stage, bronze, silver, or gold)
       migrate_schema(schema, layer)
   ```

The script will perform the full migration for the specified schema and layer.

## Contributing

This project is intended as a **proof of concept**. If you'd like to contribute or improve upon the migration logic, feel free to fork the repository and submit a pull request.

---

### Final Notes:

- Ensure that you test the migration process on a **staging environment** before applying it to a production system.
- Implement appropriate error handling and logging for better monitoring in production environments.

