import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from google.cloud import bigquery
from google.cloud import storage
from jinja2 import Template

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("plss_ddl_airflow_connection")
logger.setLevel(logging.INFO)

LOCATION = "US"

# ---------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------
CONFIG_VAR_NAME = "PLSS_PROJECT_CONFIG"
os.environ["CONFIG_PROJECT_VAR"] = CONFIG_VAR_NAME

# ---------------------------------------------------------------------
# ENV → project & bucket mapping
# ---------------------------------------------------------------------
ENV_CONFIG = {
    "dev": {
        "project_name": "edp-{env}-carema",
        "bucket_name": "cma-plss-onboarding-lan-ent-{env}",
    },
    "qa": {
        "project_name": "edp-{env}-tenants-carema",
        "bucket_name": "cma-plss-onboarding-lan-ent-{env}",
    },
    "preprod": {
        "project_name": "edp-{env}-carema",
        "bucket_name": "cma-plss-onboarding-lan-ent-{env}",
    },
    "prod": {
        "project_name": "edp-{env}-carema",
        "bucket_name": "cma-plss-onboarding-lan-ent-{env}",
    },
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def get_env_from_airflow() -> str:
    env = Variable.get("ENV", default_var=None)
    if not env:
        raise ValueError("ENV Airflow Variable must be set (dev/qa/preprod/prod)")
    env = env.lower().strip()
    if env not in ENV_CONFIG:
        raise ValueError(f"Invalid ENV '{env}', expected {list(ENV_CONFIG.keys())}")
    return env


def get_project_name(env: str) -> str:
    return ENV_CONFIG[env]["project_name"].format(env=env)


def get_bucket_name(env: str) -> str:
    return ENV_CONFIG[env]["bucket_name"].format(env=env)


def get_plss_project_config() -> Dict:
    try:
        cfg = Variable.get(CONFIG_VAR_NAME, deserialize_json=True)
    except Exception:
        raw = Variable.get(CONFIG_VAR_NAME, default_var=None)
        if not raw:
            raise ValueError(f"{CONFIG_VAR_NAME} Airflow Variable not found")
        cfg = json.loads(raw)

    if not isinstance(cfg, dict):
        raise ValueError(f"{CONFIG_VAR_NAME} must be JSON object")
    return cfg


def read_connections_from_config() -> Tuple[str, str]:
    cfg = get_plss_project_config()

    bq_conn = cfg.get("PA_BIGQUERY_CONNECTION")
    if not bq_conn:
        raise ValueError("PA_BIGQUERY_CONNECTION missing in PLSS_PROJECT_CONFIG")

    gcs_impersonate_sa = cfg.get("GCS_IMPERSONATE_SERVICE_ACCOUNT")
    if not gcs_impersonate_sa:
        raise ValueError("GCS_IMPERSONATE_SERVICE_ACCOUNT missing in PLSS_PROJECT_CONFIG")

    return bq_conn, gcs_impersonate_sa


# ---------------------------------------------------------------------
# Client builders
# ---------------------------------------------------------------------
def get_bigquery_client_from_connection(conn_id: str) -> bigquery.Client:
    logger.info(f"Creating BigQuery client using connection: {conn_id}")
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson or {}

    project = extras.get("extra_google_cloud_platform_project") or conn.schema
    if not project:
        raise ValueError(
            f"Connection '{conn_id}' must define "
            f"extra_google_cloud_platform_project in extras"
        )

    key_path = extras.get("extra_google_cloud_platform_key_path")
    if key_path:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(credentials=credentials, project=project)

    return bigquery.Client(project=project)


def get_gcs_client_with_impersonation(target_sa: str) -> storage.Client:
    logger.info(f"Creating GCS client via impersonation: {target_sa}")
    import google.auth
    from google.auth import impersonated_credentials

    source_creds, project = google.auth.default()

    target_creds = impersonated_credentials.Credentials(
        source_credentials=source_creds,
        target_principal=target_sa,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=600,
    )

    return storage.Client(credentials=target_creds, project=project)


# ---------------------------------------------------------------------
# SQL discovery
# ---------------------------------------------------------------------
def get_sql_files(
    params: dict, bucket_name: str, gcs_client: storage.Client
) -> List[str]:

    sql_dirs = params.get("sql_dirs", [])
    sql_files = params.get("sql_files", [])

    if isinstance(sql_dirs, str):
        sql_dirs = [sql_dirs]
    if isinstance(sql_files, str):
        sql_files = [sql_files]

    if not sql_dirs and not sql_files:
        raise ValueError("Provide sql_dirs and/or sql_files")

    bucket = gcs_client.bucket(bucket_name)
    result: List[str] = []

    # Auto-discover directories
    for d in sql_dirs:
        prefix = d.rstrip("/") + "/"
        blobs = bucket.list_blobs(prefix=prefix)
        found = [b.name for b in blobs if b.name.endswith(".sql")]
        logger.info(f"Found {len(found)} SQL files in {d}")
        result.extend(found)

    # Explicit files
    for f in sql_files:
        if "/" in f:
            if f.endswith(".sql"):
                result.append(f)
        else:
            for d in sql_dirs:
                result.append(f"{d.rstrip('/')}/{f}")

    # Deduplicate
    result = list(dict.fromkeys(result))
    if not result:
        raise ValueError("No SQL files resolved")

    return result


# ---------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------
def run_ddl_sql_files(**context):
    env = get_env_from_airflow()
    project = get_project_name(env)
    bucket_name = get_bucket_name(env)

    bq_conn_id, gcs_impersonate_sa = read_connections_from_config()

    bq_client = get_bigquery_client_from_connection(bq_conn_id)
    gcs_client = get_gcs_client_with_impersonation(gcs_impersonate_sa)

    params = context.get("params")
    if not params:
        raise ValueError("No params provided to DAG")

    sql_files = get_sql_files(params, bucket_name, gcs_client)
    bucket = gcs_client.bucket(bucket_name)

    execution_date = context.get("ds")

    for path in sql_files:
        sql = bucket.blob(path).download_as_text()
        rendered_sql = Template(sql).render(
            env=env,
            ENV=env,
            ds=execution_date,
        )

        logger.info(f"Executing SQL: gs://{bucket_name}/{path}")
        job = bq_client.query(rendered_sql, location=LOCATION)
        job.result()
        logger.info("SUCCESS")


# ---------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------
try:
    _env = get_env_from_airflow()
    _project_tag = get_project_name(_env)
except Exception:
    _project_tag = "edp-dev-carema"

default_args = {"start_date": datetime(2025, 11, 9)}

with DAG(
    dag_id="plss_ddl_airflow_connection",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["cvs_app_id:APM001721", f"project_id:{_project_tag}"],
) as dag:

    run_ddl = PythonOperator(
        task_id="run_ddl_sql_files",
        python_callable=run_ddl_sql_files,
        provide_context=True,
    )
