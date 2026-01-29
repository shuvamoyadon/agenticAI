#### to run from airflow , you need to pass 

#   {
#     "sql_dir": "your/sql/directory",
#     "sql_files": ["file1.sql", "file2.sql"]
    }


from datetime import datetime
from typing import List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from jinja2 import Template
from google.cloud import secretmanager

LOCATION = "US"

# ----------------------------------------------------------------------
# ENV from Airflow Variable
# In Airflow UI -> Admin -> Variables:
#   Key: ENV
#   Value: dev (or qa / prod)
# ----------------------------------------------------------------------

ENV = Variable.get("ENV")  # Fetch the environment value (dev, qa, prod) from Airflow variables

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

default_args = {
    "start_date": datetime(2025, 11, 9),
}

# Function to retrieve the secret from Google Cloud Secret Manager
def get_secret(project_id: str, secret_name: str) -> str:
    """
    Retrieves the secret from Google Cloud Secret Manager for a specific project.
    Returns:
        str: The secret payload as a string.
    """
    # Initialize the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Construct the secret version path using the provided project_id and secret_name
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    
    # Access the secret version
    secret_response = client.access_secret_version(name=name)

    # Decode and return the secret payload (usually a JSON string or plain text)
    secret_payload = secret_response.payload.data.decode("UTF-8")
    
    return secret_payload

# Fetch the secret for GCP_CONN_ID (plss_secret_dev)
# Pass the current project ID dynamically
GCP_CONN_ID_SECRET = get_secret(GCP_PROJECT_NAME, "plss_secret_dev")  # Adjust the secret name if necessary

def _get_sql_files_for_project(params: dict) -> List[str]:
    """
    Retrieve the list of SQL files from params (passed during DAG run) or auto-discover in GCS.
    """
    # SQL directory and files are passed as parameters in Airflow DAG run
    sql_dir = params.get("sql_dir")
    sql_files = params.get("sql_files", [])

    if not sql_dir:
        raise ValueError("SQL directory is not defined in the params.")
    
    if sql_files:
        # If sql_files are defined in the params, return them
        return [f"{sql_dir.rstrip('/')}/{fname}" for fname in sql_files]
    
    # Case 2: Auto-discover all SQL files under sql_dir in GCS
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID_SECRET)  # Using secret-managed GCP_CONN_ID
    prefix = sql_dir.rstrip("/") + "/"
    all_objects = gcs_hook.list(bucket_name=CONFIG_BUCKET_NAME, prefix=prefix)

    if not all_objects:
        raise ValueError(f"No files found under gs://{CONFIG_BUCKET_NAME}/{prefix} for project.")

    sql_files_to_run = [obj for obj in all_objects if obj.endswith(".sql")]

    if not sql_files_to_run:
        raise ValueError(f"No .sql files found under gs://{CONFIG_BUCKET_NAME}/{prefix}")

    return sql_files_to_run

def run_ddl_sql_files(**context):
    # BigQuery client using the secret-managed GCP_CONN_ID
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID_SECRET, location=LOCATION)  # Using secret-managed GCP_CONN_ID
    client: bigquery.Client = bq_hook.get_client(project_id=GCP_PROJECT_NAME)

    # Retrieve top-level config params
    env = ENV
    execution_date = context.get("ds")
    
    # Fetch params passed to the task
    params = context.get("params")
    
    if not params:
        raise ValueError("No params found for the task execution.")
    
    # Get the SQL files for the given params (sql_dir and sql_files)
    sql_files_to_run = _get_sql_files_for_project(params)

    # Execute SQL Files
    for sql_path in sql_files_to_run:
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID_SECRET)  # Using secret-managed GCP_CONN_ID
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
