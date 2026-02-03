# dags/sample28.py
# ============================================================
# DAG: PCT_WRITE_TO_PLSS_READ_CUSTOMER
# Purpose:
#   - Dynamically map JSON -> BigQuery columns
#   - Perform SCD1 MERGE
#   - Use GCP Secret Manager for BigQuery & GCS authentication
# ============================================================

import os
import json
import logging
from datetime import datetime
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import bigquery
from google.cloud import storage as gcs_storage
from google.cloud import secretmanager
from google.oauth2 import service_account
from jinja2 import Template

# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("plss_customer_merge")
logger.setLevel(logging.INFO)

LOCATION = "US"
JSON_COL = "Payload"

# -------------------------------------------------------------------------
# ENV CONFIG
# -------------------------------------------------------------------------
ENV_CONFIG = {
    "dev": {
        "project_name_t": "edp-dev-carema",
        "bucket_name_t": "cma-plss-onboarding-lan-ent-dev",
    },
    "qa": {
        "project_name_t": "edp-qa-tenants-carema",
        "bucket_name_t": "cma-plss-onboarding-lan-ent-qa",
    },
    "prod": {
        "project_name_t": "edp-prod-carema",
        "bucket_name_t": "cma-plss-onboarding-lan-ent-prod",
    },
}

# -------------------------------------------------------------------------
# Load ENV
# -------------------------------------------------------------------------
ENV = Variable.get("ENV").lower().strip()
if ENV not in ENV_CONFIG:
    raise ValueError(f"Invalid ENV: {ENV}")

PROJECT_NAME = ENV_CONFIG[ENV]["project_name_t"]
BUCKET_NAME = ENV_CONFIG[ENV]["bucket_name_t"]
PROJECT_ID = f"edp-{ENV}-storage"

RAW_TABLE = f"{PROJECT_ID}.edp_ent_cma_plss_onboarding_src.T_PCT_GCP_FIN_ACCOUNTS"
TARGET_TABLE = f"{PROJECT_ID}.edp_ent_cma_plss_onboarding_src.T_CUSTOMER"

CONFIG_FILE = "plssdi-hcd/plss_onboarding_platform/source/metadata/Customer.json"
SQL_TEMPLATE_PATH = "plssdi-hcd/plss_onboarding_platform/source/sql/Customer.sql"

# -------------------------------------------------------------------------
# LOAD PLSS_PROJECT_CONFIG (SECRET IDS COME FROM HERE)
# -------------------------------------------------------------------------
try:
    plss_cfg = Variable.get("PLSS_PROJECT_CONFIG", deserialize_json=True)
except Exception as e:
    raise ValueError("PLSS_PROJECT_CONFIG Airflow Variable missing or invalid") from e

SECRET_PROJECT = plss_cfg.get("SECRET_MANAGER_PROJECT", plss_cfg.get("PA_PROJECT"))
BQ_SECRET_ID = plss_cfg.get("BIGQUERY_SECRET_ID")
GCS_SECRET_ID = plss_cfg.get("GCS_SECRET_ID")
PLSS_PROJECT = plss_cfg.get("PLSS_PROJECT")

if not all([SECRET_PROJECT, BQ_SECRET_ID, GCS_SECRET_ID, PLSS_PROJECT]):
    raise ValueError("Missing required secret configuration in PLSS_PROJECT_CONFIG")

# -------------------------------------------------------------------------
# SECRET MANAGER HELPERS
# -------------------------------------------------------------------------

def get_secret(secret_id: str, project_id: str, version: str = "latest") -> str:
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
        response = secret_client.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode("UTF-8")
        logger.info(f"Retrieved secret: {secret_id} from project: {project_id}")
        return secret_value
    except Exception as e:
        logger.error(
            f"Failed to retrieve secret '{secret_id}' from project '{project_id}': {str(e)}"
        )
        raise

def create_bigquery_client() -> bigquery.Client:
    """🔐 BigQuery client using SA JSON from Secret Manager"""
    secret_json = get_secret(BQ_SECRET_ID, SECRET_PROJECT)
    sa_info = json.loads(secret_json)

    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )

    return bigquery.Client(
        project=PLSS_PROJECT,
        credentials=credentials,
        location=LOCATION,
    )


def create_gcs_client() -> gcs_storage.Client:
    """🔐 GCS client using SA JSON from Secret Manager"""
    secret_json = get_secret(GCS_SECRET_ID, SECRET_PROJECT)
    sa_info = json.loads(secret_json)

    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return gcs_storage.Client(
        project=sa_info.get("project_id", PLSS_PROJECT),
        credentials=credentials,
    )

# -------------------------------------------------------------------------
# CATEGORY-BASED FIELD LOGIC (UNCHANGED)
# -------------------------------------------------------------------------
def category_based_expr(col: str, col_type: str) -> str | None:
    category_case = "JSON_VALUE(Payload, '$.ClientCategory__c')"

    if col == "customer_id":
        return (
            f"CASE {category_case} "
            f"WHEN 'Caremark' THEN SAFE_CAST(JSON_VALUE(Payload, '$.ExternalId__c') AS {col_type}) "
            f"WHEN 'Aetna' THEN SAFE_CAST(JSON_VALUE(Payload, '$.ExternalId__c') AS {col_type}) "
            f"WHEN 'Third_Party' THEN SAFE_CAST(JSON_VALUE(Payload, '$.ExternalId__c') AS {col_type}) "
            f"END AS customer_id"
        )

    if col == "customer_nm":
        return (
            f"CASE {category_case} "
            f"WHEN 'Third_Party' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Name') AS {col_type}) "
            f"WHEN 'Caremark' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Name') AS {col_type}) "
            f"WHEN 'Aetna' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Name') AS {col_type}) "
            f"END AS customer_nm"
        )

    if col == "account_manager_nm":
        return (
            f"CASE {category_case} "
            f"WHEN 'Caremark' THEN SAFE_CAST(JSON_VALUE(Payload, '$.PBM_Account_Manager__c') AS {col_type}) "
            f"WHEN 'Aetna' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Aetna_Account_Manager__c') AS {col_type}) "
            f"END AS account_manager_nm"
        )

    return None

# -------------------------------------------------------------------------
# MAIN TASK
# -------------------------------------------------------------------------
def generate_merge_sql(**_):

    # 🔐 Secret-based clients (REPLACES BigQueryHook & GCSHook)
    bq_client = create_bigquery_client()
    gcs_client = create_gcs_client()

    # ---------------------------------------------------------------------
    # Load JSON mapping config
    # ---------------------------------------------------------------------
    bucket = gcs_client.bucket(BUCKET_NAME)
    config_blob = bucket.blob(CONFIG_FILE)
    config_data = json.loads(config_blob.download_as_text())

    mapping = {
        m["target_field"]: m.get("source_field", "")
        for m in config_data.get("field_mappings", [])
    }

    # ---------------------------------------------------------------------
    # Fetch target schema
    # ---------------------------------------------------------------------
    schema_query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.edp_ent_cma_plss_onboarding_src.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'T_CUSTOMER'
        ORDER BY ordinal_position
    """

    schema_result = list(bq_client.query(schema_query).result())
    schema_dict = {
        r["column_name"].lower(): r["data_type"].upper().split("(")[0]
        for r in schema_result
    }

    source_cols = list(schema_dict.keys())
    select_exprs: List[str] = []

    # ---------------------------------------------------------------------
    # Build SELECT expressions
    # ---------------------------------------------------------------------
    for col in source_cols:
        if col in ("orig_src_pst_dts", "source_last_process_dts"):
            continue

        col_type = schema_dict[col]
        target_field = mapping.get(col, "").strip()

        category_expr = category_based_expr(col, col_type)
        if category_expr:
            select_exprs.append(category_expr)
            continue

        if not target_field:
            select_exprs.append(f"CAST(NULL AS {col_type}) AS {col}")
        else:
            base_expr = f"JSON_VALUE(Payload, '$.{target_field}')"
            if col_type == "DATE" and col in (
                "business_effective_dt",
                "business_expiration_dt",
            ):
                select_exprs.append(
                    f"DATE(SAFE_CAST({base_expr} AS TIMESTAMP)) AS {col}"
                )
            else:
                select_exprs.append(
                    f"SAFE_CAST({base_expr} AS {col_type}) AS {col}"
                )

    select_exprs.extend(
        [
            "SAFE_CAST(JSON_VALUE(Payload, '$.CreatedDate') AS TIMESTAMP) AS orig_src_pst_dts",
            "SAFE_CAST(JSON_VALUE(Payload, '$.LastModifiedDate') AS TIMESTAMP) AS source_last_process_dts",
        ]
    )

    select_columns = ",\n    ".join(select_exprs)

    # ---------------------------------------------------------------------
    # Render SQL template
    # ---------------------------------------------------------------------
    sql_template = bucket.blob(SQL_TEMPLATE_PATH).download_as_text()
    rendered_sql = Template(sql_template).render(
        PROJECT_ID=PROJECT_ID,
        RAW_TABLE=RAW_TABLE,
        TARGET_TABLE=TARGET_TABLE,
        select_columns=select_columns,
        source_columns=source_cols,
    )

    logger.info("Executing MERGE SQL")
    bq_client.query(rendered_sql).result()
    logger.info("MERGE completed successfully")

# -------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------
default_args = {"start_date": datetime(2025, 11, 9)}

with DAG(
    dag_id="PCT_WRITE_TO_PLSS_READ_CUSTOMER",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["cvs_app_id:AP", f"project_id:{PROJECT_NAME}"],
) as dag:

    move_data = PythonOperator(
        task_id="move_data_from_PCT_to_PLSS",
        python_callable=generate_merge_sql,
    )
