# Incremental Deidentification to GCS

This repository contains an Apache Airflow DAG configuration for performing incremental data deidentification tasks using Kubernetes Pods. The tasks read configuration from JSON and YAML files, set up environment variables, and execute Kubernetes Pods to handle data deidentification operations.

## Overview

The DAG, named `incremental_deidentification_to_gcs`, is designed to:

- Read configuration details from JSON and YAML files.
- Create Kubernetes tasks for each product and region based on the configuration.
- Execute the tasks using Kubernetes Pods to perform data deidentification operations.

## Configuration Files

1. **`incremental_deid_config.json`**:
   - Contains global configuration for data deidentification.

2. **`zone_config.yaml`**:
   - Specifies the zones (product and region) along with their Kubernetes configuration and image details.

## Code Structure

### Imports

- `json`, `os`, `datetime`, `timedelta`: For handling JSON files, environment variables, and date manipulations.
- `yaml`: For reading YAML configuration files.
- `airflow.decorators`, `airflow.models.variable`, `airflow.models`, `airflow.providers.cncf.kubernetes.operators.kubernetes_pod`, `airflow.utils.dates`, `airflow.utils.task_group`: For defining and managing Airflow tasks and DAGs.

### Constants and Variables

- **`DAG_NAME`**: The name of the Airflow DAG.
- **`DEID_CONFIG_PATH`**: Path to the global deidentification configuration JSON file.
- **`ZONES_FILE_PATH`**: Path to the zone configuration YAML file.

### DAG Definition

- **DAG Name**: `incremental_deidentification_to_gcs`
- **Schedule Interval**: Runs daily at 00:25 UTC.
- **Start Date**: Defined in the zone configuration file.
- **Tags**: `DL_RAW_ZONE`
- **Default Arguments**: Includes the owner of the DAG and maximum active runs.

### Task Creation

- **`create_kubernetes_task_for_product_region`**:
  - Creates a Kubernetes Pod task for a given product and region.
  - Configures the Pod with environment variables and Kubernetes configurations.
  - Uses the `KubernetesPodOperator` to manage Kubernetes Pods.

### Task Groups

- **`kubernetes_task_for_product_region`**:
  - Creates and manages Kubernetes Pod tasks for all products and regions defined in the zone configuration file.
