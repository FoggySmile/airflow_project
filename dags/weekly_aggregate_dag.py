from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weekly_aggregate',
    default_args=default_args,
    description='Compute weekly aggregates using Spark',
    schedule='0 7 * * *',  # Каждый день в 7:00 утра
    catchup=False,
) as dag:

    aggregate_task = SparkSubmitOperator(
        task_id='spark_weekly_aggregate',
        application='/opt/airflow/dags/spark_aggregate.py',
        name='spark_weekly_aggregate',
        conn_id='spark_default',
        verbose=True,
        conf={'spark.master': 'yarn'},
        env_vars={
            'HADOOP_CONF_DIR': '/path/to/hadoop/conf',
            'YARN_CONF_DIR': '/path/to/yarn/conf',
        },
        application_args=["{{ ds }}"],
        dag=dag,
    )
