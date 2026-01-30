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

# ----------------------------------------------------------------------
# Environment setup
# ----------------------------------------------------------------------
os.environ["CONFIG_PROJECT_VAR"] = "PLSS_PROJECT_CONFIG"

# ENV configuration - supports dev, qa, preprod, prod
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


def get_env_from_airflow() -> str:
    """Get ENV variable from Airflow Variables."""
    env = Variable.get("ENV", default_var=None)
    if not env:
        raise ValueError(
            f"ENV variable is not set in Airflow Variables. Must be one of: {list(ENV_CONFIG.keys())}"
        )
    env = env.lower().strip()
    if env not in ENV_CONFIG:
        raise ValueError(f"Invalid ENV value: '{env}'. Must be one of: {list(ENV_CONFIG.keys())}")
    return env


def get_project_name(env: str) -> str:
    return ENV_CONFIG[env]["project_name"].format(env=env)


def get_bucket_name(env: str) -> str:
    return ENV_CONFIG[env]["bucket_name"].format(env=env)


def get_plss_project_config() -> Dict:
    """Get PLSS_PROJECT_CONFIG from Airflow Variables."""
    try:
        config = Variable.get(os.environ["CONFIG_PROJECT_VAR"], deserialize_json=True)
    except Exception:
        config_str = Variable.get(os.environ["CONFIG_PROJECT_VAR"], default_var=None)
        if not config_str:
            raise ValueError(f"{os.environ['CONFIG_PROJECT_VAR']} not found in Airflow Variables")
        config = json.loads(config_str)

    if not isinstance(config, dict):
        raise ValueError(f"{os.environ['CONFIG_PROJECT_VAR']} must be a dict. Got: {type(config)}")

    return config


def _read_gcp_connections_from_config() -> Tuple[str, str, str]:
    """
    Reads connection ids + impersonation SA from PLSS_PROJECT_CONFIG.
    Expected keys (based on your second code):
      - PA_BIGQUERY_CONNECTION (required)
      - PA_GCS_CONNECTION (optional; fallback to PA_BIGQUERY_CONNECTION)
      - GCS_IMPERSONATE_SERVICE_ACCOUNT (required for impersonation)
    """
    cfg = get_plss_project_config()

    pa_bq_connection = cfg.get("PA_BIGQUERY_CONNECTION")
    if not pa_bq_connection:
        raise ValueError("PA_BIGQUERY_CONNECTION must be configured in PLSS_PROJECT_CONFIG")

    pa_gcs_connection = cfg.get("PA_GCS_CONNECTION", pa_bq_connection)
    gcs_impersonate_sa = cfg.get("GCS_IMPERSONATE_SERVICE_ACCOUNT")

    if not gcs_impersonate_sa:
        raise ValueError("GCS_IMPERSONATE_SERVICE_ACCOUNT must be configured in PLSS_PROJECT_CONFIG")

    return pa_bq_connection, pa_gcs_connection, gcs_impersonate_sa


def get_bigquery_client_from_connection(bq_conn_id: str) -> bigquery.Client:
    """
    Create BigQuery client using the Airflow GCP connection extras:
      - extra_google_cloud_platform_key_path
      - extra_google_cloud_platform_project
    """
    logger.info(f"Creating BigQuery client using connection: {bq_conn_id}")
    conn = BaseHook.get_connection(bq_conn_id)
    extras = conn.extra_dejson or {}

    key_path = extras.get("extra_google_cloud_platform_key_path")
    project = extras.get("extra_google_cloud_platform_project")

    if not project:
        raise ValueError(
            f"Connection '{bq_conn_id}' missing extra_google_cloud_platfoarm_project in extras"
        )

    if key_path:
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(credentials=credentials, project=project)

    # fallback to default creds of the environment
    return bigquery.Client(project=project)


def get_gcs_storage_client_with_impersonation(target_service_account: str) -> storage.Client:
    """
    Create GCS client using ADC source credentials + impersonate target SA.
    """
    logger.info(f"Creating GCS client via impersonation: {target_service_account}")
    import google.auth
    from google.auth import impersonated_credentials

    source_credentials, project = google.auth.default()

    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_service_account,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=500,
    )

    return storage.Client(credentials=target_credentials, project=project)


def get_sql_files_for_project(params: dict, bucket_name: str, gcs_client: storage.Client) -> List[str]:
    """
    Retrieve SQL files from GCS directories.

    Supports three input formats:
    1. Auto-discover all files: {"sql_dirs": ["path1", "path2"]}
    2. Specific files (full paths): {"sql_files": ["path1/file1.sql", "path2/file2.sql"]}
    3. Mixed: {"sql_dirs": ["path1"], "sql_files": ["path2/file1.sql", "path2/file2.sql"]}
    """
    sql_dirs = params.get("sql_dirs", [])
    sql_files = params.get("sql_files", [])

    if isinstance(sql_dirs, str):
        sql_dirs = [sql_dirs]
    if isinstance(sql_files, str):
        sql_files = [sql_files]

    if not isinstance(sql_dirs, list):
        raise ValueError(f"sql_dirs must be a string or list, got {type(sql_dirs)}")
    if not isinstance(sql_files, list):
        raise ValueError(f"sql_files must be a string or list, got {type(sql_files)}")

    if not sql_dirs and not sql_files:
        raise ValueError("Either sql_dirs or sql_files must be provided.")

    all_sql_files: List[str] = []
    bucket = gcs_client.bucket(bucket_name)

    # Format 1: Auto-discover all .sql files in sql_dirs
    for sql_dir in sql_dirs:
        if not sql_dir:
            continue
        prefix = sql_dir.rstrip("/") + "/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        names = [b.name for b in blobs]
        found = [n for n in names if n.endswith(".sql")]
        all_sql_files.extend(found)
        logger.info(f"[INFO] Found {len(found)} SQL file(s) in directory: {sql_dir}")

    # Format 2 & 3: Handle explicit sql_files
    for sql_file in sql_files:
        if not sql_file:
            continue

        # If full path
        if "/" in sql_file:
            if sql_file.endswith(".sql"):
                all_sql_files.append(sql_file)
                logger.info(f"[INFO] Added SQL file (full path): {sql_file}")
            else:
                logger.warning(f"[WARNING] Skipping non-SQL file: {sql_file}")
            continue

        # If only filename, combine with sql_dirs
        if sql_dirs:
            for sql_dir in sql_dirs:
                full_path = f"{sql_dir.rstrip('/')}/{sql_file}"
                all_sql_files.append(full_path)
                logger.info(f"[INFO] Added SQL file: {full_path}")
        else:
            # no dirs provided; treat as full path
            if sql_file.endswith(".sql"):
                all_sql_files.append(sql_file)
                logger.info(f"[INFO] Added SQL file (as full path): {sql_file}")

    # Remove duplicates (preserve order)
    all_sql_files = list(dict.fromkeys(all_sql_files))

    if not all_sql_files:
        raise ValueError(f"No SQL files found. sql_dirs={sql_dirs}, sql_files={sql_files}")

    logger.info(f"[INFO] Total SQL files to execute: {len(all_sql_files)}")
    return all_sql_files


def run_ddl_sql_files(**context):
    """Main function to execute SQL files from GCS."""
    runtime_env = get_env_from_airflow()
    runtime_bucket_name = get_bucket_name(runtime_env)

    # Read connection ids + impersonation SA from PLSS_PROJECT_CONFIG
    bq_conn_id, _gcs_conn_id, gcs_impersonate_sa = _read_gcp_connections_from_config()

    # Build clients
    bq_client = get_bigquery_client_from_connection(bq_conn_id)
    gcs_client = get_gcs_storage_client_with_impersonation(gcs_impersonate_sa)

    params = context.get("params")
    if not params:
        raise ValueError("No params found for the task execution.")

    sql_files_to_run = get_sql_files_for_project(params, runtime_bucket_name, gcs_client)

    bucket = gcs_client.bucket(runtime_bucket_name)
    execution_date = context.get("ds")

    for sql_path in sql_files_to_run:
        blob = bucket.blob(sql_path)
        raw_sql = blob.download_as_text(encoding="utf-8")

        rendered_sql = Template(raw_sql).render(
            env=runtime_env,
            ENV=runtime_env,
            ds=execution_date,
        )

        logger.info(f"[INFO] Executing SQL from: gs://{runtime_bucket_name}/{sql_path}")
        job = bq_client.query(rendered_sql, location=LOCATION)
        job.result()
        logger.info(f"[SUCCESS] Executed SQL: gs://{runtime_bucket_name}/{sql_path}")


# ----------------------------------------------------------------------
# Safe fallback for DAG parsing
# ----------------------------------------------------------------------
try:
    _env_for_tags = get_env_from_airflow()
    _project_for_tags = get_project_name(_env_for_tags)
except Exception:
    _project_for_tags = "edp-dev-carema"  # safe default just for DAG tag rendering

default_args = {"start_date": datetime(2025, 11, 9)}

with DAG(
    dag_id="plss_ddl_airflow_connection",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["cvs_app_id:APM001721", f"project_id:{_project_for_tags}"],
) as dag:
    run_ddl = PythonOperator(
        task_id="run_ddl_sql_files",
        python_callable=run_ddl_sql_files,
        provide_context=True,
    )
