from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="spark_pi_on_kubernetes",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "kubernetes", "example"],
) as dag:
    submit_spark_pi = SparkKubernetesOperator(
        task_id="submit_spark_pi_task",
        application_file="spark-jobs/spark.yaml",  # Path to the YAML file inside the Airflow containers
        namespace="spark",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )
