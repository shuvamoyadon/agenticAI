from datetime import datetime
from typing import List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from jinja2 import Template

# Define GCP connection ID
GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"

# ----------------------------------------------------------------------
# ENV from Airflow Variable
# In Airflow UI -> Admin -> Variables:
#   Key: ENV
#   Value: dev (or qa / prod)
# ----------------------------------------------------------------------

ENV = Variable.get("ENV").lower()  # Fetch and convert the environment value to lowercase (dev, qa, prod)

# Define the configuration for different environments (dev, qa, prod) with dynamic {env} placeholders
ENV_CONFIG = {
    "dev": {
        "project_name": "edp-{env}-carema",  # project_name template with {env}
        "bucket_name": "cma-plss-onboarding-lan-ent-{env}"  # bucket_name template with {env}
    },
    "qa": {
        "project_name": "edp-{env}-tenants-carema",  # project_name template with {env}
        "bucket_name": "cma-plss-onboarding-lan-ent-{env}"  # bucket_name template with {env}
    },
    "prod": {
        "project_name": "edp-{env}-carema",  # project_name template with {env}
        "bucket_name": "cma-plss-onboarding-lan-ent-{env}"  # bucket_name template with {env}
    }
}

# Dynamically select the config based on the current environment (dev, qa, prod)
if ENV in ENV_CONFIG:
    config = ENV_CONFIG[ENV]
else:
    raise ValueError(f"Unknown environment: {ENV}")

# Use Jinja templating to replace the {env} placeholder in project_name and bucket_name
project_name_template = Template(config["project_name"])
bucket_name_template = Template(config["bucket_name"])

# Replace {env} with the actual environment value (dev, qa, prod)
GCP_PROJECT_NAME = project_name_template.render(env=ENV)
CONFIG_BUCKET_NAME = bucket_name_template.render(env=ENV)

# Log the project name and bucket name for debugging purposes
print(f"Generated GCP Project Name: {GCP_PROJECT_NAME}")
print(f"Generated GCP Bucket Name: {CONFIG_BUCKET_NAME}")

default_args = {
    "start_date": datetime(2025, 11, 9),
}

def _get_sql_files_for_project(params: dict) -> List[str]:
    """
    Retrieve the list of SQL files from multiple directories (passed during DAG run) or auto-discover in GCS.
    """
    # Multiple SQL directories and files are passed as parameters in Airflow DAG run
    sql_dirs = params.get("sql_dirs", [])  # List of SQL directories
    sql_files = params.get("sql_files", [])

    if not sql_dirs and not sql_files:
        raise ValueError("Either sql_dirs or sql_files must be provided.")
    
    all_sql_files = []

    if sql_files:
        # If sql_files are defined in the params, return them
        for sql_dir in sql_dirs:
            all_sql_files.extend([f"{sql_dir.rstrip('/')}/{fname}" for fname in sql_files])
    else:
        # Case 2: Auto-discover all SQL files under sql_dirs in GCS if sql_files is not provided
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)  # Using the Airflow GCP connection ID
        for sql_dir in sql_dirs:
            prefix = sql_dir.rstrip("/") + "/"
            all_objects = gcs_hook.list(bucket_name=CONFIG_BUCKET_NAME, prefix=prefix)

            if not all_objects:
                raise ValueError(f"No files found under gs://{CONFIG_BUCKET_NAME}/{prefix} for project.")

            sql_files_to_run = [obj for obj in all_objects if obj.endswith(".sql")]
            all_sql_files.extend(sql_files_to_run)

    if not all_sql_files:
        raise ValueError(f"No .sql files found under gs://{CONFIG_BUCKET_NAME}/ in the specified directories.")

    return all_sql_files

def run_ddl_sql_files(**context):
    # BigQuery client using the Airflow GCP connection ID
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)  # Use the Airflow GCP connection ID
    client: bigquery.Client = bq_hook.get_client(project_id=GCP_PROJECT_NAME)

    # Retrieve top-level config params
    env = ENV
    execution_date = context.get("ds")
    
    # Fetch params passed to the task
    params = context.get("params")
    
    if not params:
        raise ValueError("No params found for the task execution.")
    
    # Get the SQL files for the given params (sql_dirs and sql_files)
    sql_files_to_run = _get_sql_files_for_project(params)

    # Execute SQL Files
    for sql_path in sql_files_to_run:
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)  # Using the Airflow GCP connection ID
        content = gcs_hook.download(
            bucket_name=CONFIG_BUCKET_NAME,
            object_name=sql_path
        )
        raw_sql = content.decode("utf-8")

        # Render the SQL template with only 'env' and 'execution_date'
        rendered_sql = Template(raw_sql).render(
            env=env,
            ds=execution_date,  # You can keep 'execution_date' if required by your SQL template
        )

        # Execute the SQL in BigQuery
        print("rendered_sql", rendered_sql)
        job = client.query(rendered_sql, location=LOCATION)
        job.result()

        print(f"[SUCCESS] Executed SQL => gs://{CONFIG_BUCKET_NAME}/{sql_path} (env={env})")


# ======================================================================
# DAG
# ======================================================================

with DAG(
    dag_id="plss_ddl_1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["cvs_app_id:AP", f"project_id:{GCP_PROJECT_NAME}"],
) as dag:

    run_ddl = PythonOperator(
        task_id="run_ddl_sql_files",
        python_callable=run_ddl_sql_files,
        provide_context=True,
    )
