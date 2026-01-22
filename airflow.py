from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from pathlib import Path
from jinja2 import Template


DAG_ID = "bq_sql_file_run_env_only"


def run_bigquery_sql_file(**context):
    # ONLY env is passed from DAG
    env = context["params"]["env"]

    # Read SQL file from dags/sql directory
    sql_file_path = Path(_file_).parent / "sql" / "my_query.sql"
    sql_text = sql_file_path.read_text()

    # Render only env into SQL
    rendered_sql = Template(sql_text).render(params={"env": env})

    # Execute query using BigQueryHook connection
    bq_hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",  # BigQuery connection from Airflow UI
        use_legacy_sql=False
    )

    job = bq_hook.insert_job(
        configuration={
            "query": {
                "query": rendered_sql,
                "useLegacySql": False
            }
        }
    )

    print(f"BigQuery Job submitted successfully. Job ID: {job.job_id}")
    return job.job_id


with DAG(
    dag_id=DAG_ID,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "sql-file", "env-only"]
) as dag:

    run_sql = PythonOperator(
        task_id="run_bigquery_sql_from_file",
        python_callable=run_bigquery_sql_file,
        provide_context=True,
        params={
            # ONLY env passed from DAG Run Config (Trigger DAG with {"env":"dev"} etc.)
            "env": "{{ dag_run.conf.get('env', 'dev') }}"
        }
    )

    run_sql
