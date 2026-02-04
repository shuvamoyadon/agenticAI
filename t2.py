from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import bigquery
from google.cloud import storage as gcs_storage
from google.cloud import secretmanager
from google.oauth2 import service_account

from datetime import datetime
from jinja2 import Template
from typing import List
import json
import logging

# --------------------------------------------------
# Logging
# --------------------------------------------------
FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("plss_customer_product_entitlement")
logger.setLevel(logging.INFO)

LOCATION = "US"
JSON_COL = "Payload"

# --------------------------------------------------
# CONFIG / CONSTANTS
# --------------------------------------------------
ENV = Variable.get("ENV")

PROJECT_ID = f"edp-{ENV}-storage"

ENV_CONFIG = {
    "dev": {
        "project_name_t": "edp-dev-carema",
        "bucket_name_t": "cma-plss-onboarding-lan-ent-dev"
    },
    "qa": {
        "project_name_t": "edp-qa-tenants-carema",
        "bucket_name_t": "cma-plss-onboarding-lan-ent-qa"
    },
    "prod": {
        "project_name_t": "edp-prod-carema",
        "bucket_name_t": "cma-plss-onboarding-lan-ent-prod"
    }
}

config = ENV_CONFIG.get(ENV)
PROJECT_NAME = config["project_name_t"]
BUCKET_NAME = config["bucket_name_t"]

RAW_TABLE = f"{PROJECT_ID}.edp_ent_cma_plss_onboarding_src.T_PCT_GCP_FIN_"
TARGET_TABLE = f"{PROJECT_ID}.edp_ent_cma_plss_onboarding_src.T_CUSTOMER_PRODUCT_ENTITLEMENT"

CONFIG_FILE = "plssdi-hcd/plss_onboarding_platform/source/metadata/Customer_Product_Entitlement.json"
SQL_TEMPLATE_PATH = "plssdi-hcd/plss_onboarding_platform/source/sql/Customer_Product_Entitlement.sql"

default_args = {
    "start_date": datetime(2025, 11, 9)
}

# ============================================================
# 🔐 ADDED: LOAD SECRET CONFIG
# ============================================================
plss_cfg = Variable.get("PLSS_PROJECT_CONFIG", deserialize_json=True)

SECRET_PROJECT = plss_cfg.get("SECRET_MANAGER_PROJECT", plss_cfg.get("PA_PROJECT"))
BQ_SECRET_ID = plss_cfg.get("BIGQUERY_SECRET_ID")
GCS_SECRET_ID = plss_cfg.get("GCS_SECRET_ID")
PLSS_PROJECT = plss_cfg.get("PLSS_PROJECT")

if not all([SECRET_PROJECT, BQ_SECRET_ID, GCS_SECRET_ID, PLSS_PROJECT]):
    raise ValueError("Missing required secret configuration in PLSS_PROJECT_CONFIG")

# ============================================================
# 🔐 ADDED: SECRET MANAGER HELPERS
# ============================================================
def get_secret(secret_id: str, project_id: str, version: str = "latest") -> str:
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
        response = secret_client.access_secret_version(
            request={"name": name}
        )
        secret_value = response.payload.data.decode("UTF-8")
        logger.info(
            f"Retrieved secret '{secret_id}' from project '{project_id}'"
        )
        return secret_value

    except Exception as e:
        logger.error(
            f"Failed to retrieve secret '{secret_id}' from project '{project_id}': {str(e)}"
        )
        raise


def create_bigquery_client() -> bigquery.Client:
    """🔐 ADDED: BigQuery client using Secret Manager"""
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
    """🔐 ADDED: GCS client using Secret Manager"""
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

# ==================================================
# CATEGORY-BASED FIELD LOGIC (UNCHANGED)
# ==================================================
def category_based_expr(col: str, col_type: str) -> str | None:

    category_case = "JSON_VALUE(Payload, '$.Client_Category__c')"

    if col == "customer_population_group_id":
        return (
            f"CASE {category_case} "
            f"WHEN 'Caremark' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Grouper_Id__c') AS {col_type}) "
            f"WHEN 'Aetna' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Grouper_Id__c') AS {col_type}) "
            f"WHEN 'Third_Party' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Grouper__c') AS {col_type}) "
            f"END AS customer_population_group_id"
        )

    return None

# ==================================================
# MAIN FUNCTION
# ==================================================
def generate_merge_sql(**kwargs):

    # 🔐 CHANGED: replaced BigQueryHook
    client = create_bigquery_client()

    # 🔐 CHANGED: replaced GCSHook
    gcs_client = create_gcs_client()
    bucket = gcs_client.bucket(BUCKET_NAME)

    # Load mapping config
    config_blob = bucket.blob(CONFIG_FILE)
    config_data = json.loads(config_blob.download_as_text())

    mapping = {
        m["target_field"]: m.get("source_field", "")
        for m in config_data.get("field_mappings", [])
    }

    # Fetch schema
    schema_query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.edp_ent_cma_plss_onboarding_src.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'T_CUSTOMER_PRODUCT_ENTITLEMENT'
        ORDER BY ordinal_position
    """

    schema_result = list(client.query(schema_query).result())
    schema_dict = {
        row["column_name"]: row["data_type"].upper()
        for row in schema_result
    }

    source_cols = [c.lower() for c in schema_dict.keys()]
    select_exprs: List[str] = []

    for col in source_cols:
        target_field = mapping.get(col, "").strip()
        col_type = schema_dict[col.upper()]

        if col in ("orig_src_pst_dts", "source_last_process_dts"):
            continue

        category_expr = category_based_expr(col, col_type)
        if category_expr:
            select_exprs.append(category_expr)
            continue

        if not target_field:
            expr = f"CAST(NULL AS {col_type}) AS {col}"
        else:
            base_expr = (
                f"COALESCE("
                f"JSON_VALUE({JSON_COL}, '$.{target_field}.value'), "
                f"JSON_VALUE({JSON_COL}, '$.{target_field}')"
                f")"
            )

            if col_type == "DATE" and col in (
                "business_effective_dt",
                "business_expiration_dt"
            ):
                expr = f"DATE(SAFE_CAST({base_expr} AS TIMESTAMP)) AS {col}"
            else:
                expr = f"SAFE_CAST({base_expr} AS {col_type}) AS {col}"

        select_exprs.append(expr)

    select_exprs.append(
        f"SAFE_CAST(JSON_VALUE({JSON_COL}, '$.CreatedDate') AS TIMESTAMP) AS orig_src_pst_dts"
    )
    select_exprs.append(
        f"SAFE_CAST(JSON_VALUE({JSON_COL}, '$.LastModifiedDate') AS TIMESTAMP) AS source_last_process_dts"
    )

    select_columns = ",\n".join(select_exprs)

    # Load SQL template
    sql_template = bucket.blob(SQL_TEMPLATE_PATH).download_as_text()
    template = Template(sql_template)

    rendered_sql = template.render(
        PROJECT_ID=PROJECT_ID,
        select_columns=select_columns,
        source_columns=source_cols,
        RAW_TABLE=RAW_TABLE,
        TARGET_TABLE=TARGET_TABLE,
    )

    logger.info("Executing MERGE SQL")
    client.query(rendered_sql).result()
    logger.info("MERGE completed successfully.")

# ==================================================
# DAG DEFINITION
# ==================================================
dag = DAG(
    "PCT_WRITE_TO_PLSS_READ_CUSTOMER_PRODUCT_ENTITLEMENT",
    default_args=default_args,
    schedule_interval=None,
    description="Dynamic JSON → BigQuery mapping + SCD1 merge using sf_account_id",
    tags=["app_id:APM7121", f"project_id:{PROJECT_NAME}"],
)

build_insert_task = PythonOperator(
    task_id="build_and_execute_mapping_query",
    python_callable=generate_merge_sql,
    dag=dag,
)
