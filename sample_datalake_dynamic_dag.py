import json
import os
from datetime import datetime
from datetime import timedelta

import yaml
from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from pipelines.raw_zone.zone_configurator.zone_environment_configurator import (
    EnvironmentVariables,
)


with DAG(
    DAG_NAME,
    schedule_interval="25 0 * * *",
    start_date=common_start_date,
    tags=["DL_RAW_ZONE"],
    default_args={"owner": "Hassan"},
    max_active_runs=1,
    catchup=True,
) as dag:

    def create_kubernetes_task_for_product_region(
        product_name, region_name, kube_config_name, image_data_gcs_deidentification
    ):
        """
        Create a Kubernetes task for the specified product and region.

        Args:
            product_name (str): The name of the product.
            region_name (str): The name of the region.
        """

        env_variables_instance = EnvironmentVariables()
        env_variables_instance.set_selected_zone(
            product_name, region_name, kube_config_name
        )

        global_zone_cloud_storage_bucket_name = (
            env_variables_instance.get_global_zone_cloud_storage_bucket_name()
        )
        global_zone_google_cloud_project_name = (
            env_variables_instance.get_global_zone_google_cloud_project_name()
        )
        global_zone_bigquery_dataset_name = (
            env_variables_instance.get_global_zone_bigquery_dataset_name()
        )

        cloud_storage_bucket_name = (
            env_variables_instance.get_cloud_storage_bucket_name()
        )
        big_query_dataset_name = env_variables_instance.get_bigquery_dataset_name()
        google_cloud_project_name = (
            env_variables_instance.get_google_cloud_project_name()
        )
        google_cloud_service_account_key = (
            env_variables_instance.get_google_cloud_service_account_key()
        )

        id_hashing_key = env_variables_instance.get_id_hashing_key()

        data_namespace = env_variables_instance.get_data_namespace()

        return KubernetesPodOperator(
            task_id=f"incremental_de_task_{product_name}_{region_name}",
            name=f"incremental_de-pod-{product_name}_{region_name}",
            namespace=data_namespace,
            image=image_data_gcs_deidentification,
            in_cluster=False,
            is_delete_operator_pod=True,
            env_vars={
                "extract_start_date": "{{ macros.ds_add(ds, -1) }}",
                "extract_end_date": "{{ ds }}",
                "selected_days": "1",
                "global_deid_config": json.dumps(deid_config),
                "cloud_storage_bucket_name": cloud_storage_bucket_name,
                "google_cloud_project_name": google_cloud_project_name,
                "big_query_dataset_name": big_query_dataset_name,
                "global_id_hashing_key": id_hashing_key,
                "product_name": product_name,
                "truncate_load_flag": "False",
                "region_name": region_name,
                "global_zone_cloud_storage_bucket_name": global_zone_cloud_storage_bucket_name,
                "global_zone_bigquery_dataset_name": global_zone_bigquery_dataset_name,
                "global_zone_google_cloud_project_name": global_zone_google_cloud_project_name,
                "google_cloud_service_account_key": google_cloud_service_account_key,
            },
            config_file=f"/home/airflow/gcs/dags/{kube_config_name}.kubeconfig",
        )

    with TaskGroup(
        "kubernetes_task_for_product_region"
    ) as kubernetes_task_for_product_region:
        tasks = [
            create_kubernetes_task_for_product_region(
                product_name,
                region_name,
                kube_config_name,
                image_data_gcs_deidentification,
            )
            for product_name, region_name, kube_config_name, image_data_gcs_deidentification in zones
        ]
