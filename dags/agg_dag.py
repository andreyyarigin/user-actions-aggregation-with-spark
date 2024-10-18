from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    'weekly_spark_agg',
    default_args=default_args,
    description='run spark_agg_task daily at 7:00',
    schedule_interval='0 7 * * *', 
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_spark_agg_task = DockerOperator(
        task_id='spark_agg_task',
        image='spark_agg',  
        api_version='auto',
        auto_remove=True,
        command='spark-submit /workdir/spark_agg_script.py "{{ ds }}"',
        docker_url='tcp://docker-socket-proxy:2375',
        network_mode='bridge', 
        mount_tmp_dir=False,
        mounts=[ 
            Mount( 
                source="/Users/andreyyarigin/projects/user-actions-aggregation-with-spark/data/input", 
                target="/opt/spark/input/",
                type="bind",
            ),
            Mount( 
                source="/Users/andreyyarigin/projects/user-actions-aggregation-with-spark/data/daily_agg", 
                target="/opt/spark/daily_agg/",
                type="bind",
            ),
            Mount(
                source="/Users/andreyyarigin/projects/user-actions-aggregation-with-spark/data/output",
                target="/opt/spark/output/",
                type="bind",
            )
        ],
    )

    run_spark_agg_task
