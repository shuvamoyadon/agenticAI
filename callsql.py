import os
import json
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
from google.cloud import secretmanager
from google.oauth2 import service_account
from jinja2 import Template

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plss_sql_executor")

LOCATION = "US"
SQL_FOLDER = os.path.join(os.path.dirname(__file__), "sql")

# ---------------- Environment ----------------
def get_env() -> str:
    env = Variable.get("env", default_var=None)
    if not env:
        raise ValueError("Airflow Variable 'env' must be set")
    return env.lower().strip()

def get_project_name(env: str) -> str:   # <-- only change this to put the correct project name
    return f"edp-{env}-carema"

# ---------------- Secret Manager ----------------
def get_secret(secret_id: str, project_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def create_bigquery_client(secret_json: str, project_id: str):
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(secret_json),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(credentials=credentials, project=project_id)

# ---------------- Main Task ----------------
def run_sql_file(**context):
    env = get_env()
    project_id = get_project_name(env)

    # Airflow Variables
    secret_project = Variable.get("SECRET_MANAGER_PROJECT")
    bq_secret_id = Variable.get("BIGQUERY_SECRET_ID")

    # Create BQ client
    secret_json = get_secret(bq_secret_id, secret_project)
    bq_client = create_bigquery_client(secret_json, project_id)

    # SQL file name passed via params
    sql_file_name = context["params"]["sql_file"]
    sql_path = os.path.join(SQL_FOLDER, sql_file_name)

    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    # Read & render SQL
    with open(sql_path, "r") as f:
        raw_sql = f.read()

    rendered_sql = Template(raw_sql).render(
        env=env,
        project_id=project_id,
        ds=context.get("ds")
    )

    # Execute in BigQuery
    job = bq_client.query(rendered_sql, location=LOCATION)
    job.result()

    logger.info(f"Successfully executed {sql_file_name}")

# ---------------- DAG ----------------
default_args = {
    "start_date": datetime(2025, 11, 9)
}

with DAG(
    dag_id="plss_sql_execute",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_ddl = PythonOperator(
        task_id="run_sql_file",
        python_callable=run_sql_file,
        params={
            "sql_file": "your_script.sql"   # <-- only change this
        }
    )
