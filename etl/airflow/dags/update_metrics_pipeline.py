import os
import sys

# It is required to add custom python module location into path before importing module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'end_date': None,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id="update_metrics",
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=False,
         tags=None) as dag:
    update_metrics = SparkKubernetesOperator(
        task_id='update_metrics_step',
        namespace="default",
        application_file="update_metrics_step.yaml",
        kubernetes_conn_id="kubernetes_in_cluster",
        do_xcom_push=True,
        dag=dag,
    )