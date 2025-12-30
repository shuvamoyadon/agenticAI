MERGE INTO `playground-s-11-791f6efe.test.CUSTOMER_PRODUCT_CONFIGURATION` T
USING (

/* ============================================================
   SOURCE + FLATTEN
   ============================================================ */

WITH exploded AS (
  SELECT
    CASE
      WHEN UPPER(line_of_business) = 'THIRD_PARTY' THEN '3PY'
      WHEN UPPER(line_of_business) = 'CAREMARK'    THEN 'PBM'
      WHEN UPPER(line_of_business) = 'AETNA'       THEN 'HCB'
      ELSE line_of_business
    END AS business_unit_cd,
    ID,
    Product_Association_ID,
    CreatedDate,
    LastModifiedDate,
    JSON_QUERY_ARRAY(Payload) AS payload_array
  FROM `playground-s-11-791f6efe.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION`
  WHERE indicator_flag = 'N'
),

flattened AS (
  SELECT
    business_unit_cd,
    ID,
    Product_Association_ID,
    CreatedDate,
    LastModifiedDate,
    REGEXP_REPLACE(
      JSON_VALUE(el,'$.Attribute_Category__c'),
      r'\\u0026amp;|&amp;',
      '&'
    ) AS client_category,
    el AS json_object
  FROM exploded
  CROSS JOIN UNNEST(payload_array) el
),

/* ============================================================
   BILLING KEY/VALUE EXTRACTION
   ============================================================ */

billing_kv AS (
  SELECT
    ID,
    Product_Association_ID,

    REGEXP_REPLACE(
      REGEXP_REPLACE(JSON_VALUE(json_object,'$.AttributeName__c'), r'[^A-Za-z0-9 ]',''),
      r' +','_'
    ) AS key_name,

    JSON_VALUE(json_object,'$.Attribute_Value__c') AS raw_val,
    json_object
  FROM flattened
  WHERE client_category = 'Billing'
),

/* ============================================================
   BILLING STRING AGGREGATION
   ============================================================ */

billing_pairs AS (
  SELECT
    ID,
    Product_Association_ID,

    STRING_AGG(
      CASE
        WHEN key_name = 'Contracted_LOB_Sub_Category' THEN
          CONCAT(
            '"Contracted_LOB_Sub_Category":',
            TO_JSON_STRING(
              ARRAY(
                SELECT REGEXP_REPLACE(TRIM(v), r' +','_')
                FROM UNNEST(SPLIT(raw_val,';')) v
                WHERE TRIM(v) <> ''
              )
            )
          )
        ELSE
          CONCAT('"',key_name,'":',IF(raw_val IS NULL,'null',TO_JSON_STRING(raw_val)))
      END,
      ','
    ) AS kv_string,

    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by

  FROM billing_kv
  GROUP BY ID, Product_Association_ID
),

/* ============================================================
   BILLING FINAL JSON (ARRAY OF 1 OBJECT)
   ============================================================ */

billing_agg AS (
  SELECT
    ID,
    Product_Association_ID,

    TO_JSON(
      ARRAY(
        SELECT PARSE_JSON(
          CONCAT(
            '{',
            kv_string,
            ',"Attribute_Category__c":"Billing"',
            ',"Client_External_Id__c":"',Client_External_Id__c,'"',
            ',"Configured_Product__c":"',Configured_Product__c,'"',
            ',"Grouper_ID__c":"',Grouper_ID__c,'"',
            ',"Product_Association_ID":"',Product_Association_ID,'"',
            ',"line_of_business":"',line_of_business,'"',
            ',"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}'
          )
        )
      )
    ) AS billing_detail
  FROM billing_pairs
),

/* ============================================================
   FINAL AGGREGATION (NO JSON IN GROUP BY)
   ============================================================ */

agg AS (
  SELECT
    f.business_unit_cd,
    f.ID AS sf_product_configuration_id,
    f.Product_Association_ID AS sf_product_association_id,
    f.CreatedDate,
    f.LastModifiedDate,
    CPE.customer_id,

    TO_JSON(ARRAY_AGG(IF(client_category='Product Level', json_object,NULL) IGNORE NULLS)) AS product_level,
    TO_JSON(ARRAY_AGG(IF(client_category='Client Type', json_object,NULL) IGNORE NULLS)) AS client_type,
    ANY_VALUE(b.billing_detail) AS billing_detail,
    TO_JSON(ARRAY_AGG(IF(client_category='Program Rules', json_object,NULL) IGNORE NULLS)) AS program_rules,
    TO_JSON(ARRAY_AGG(IF(client_category='Assigned Owner & Contact Information', json_object,NULL) IGNORE NULLS)) AS assigned_owner_contact_info,
    TO_JSON(ARRAY_AGG(IF(client_category='Client Contract', json_object,NULL) IGNORE NULLS)) AS client_contract,
    TO_JSON(ARRAY_AGG(IF(client_category='Account Management', json_object,NULL) IGNORE NULLS)) AS account_management,
    TO_JSON(ARRAY_AGG(IF(client_category='ROI/PGs', json_object,NULL) IGNORE NULLS)) AS ROI_performance_guarantees,
    TO_JSON(ARRAY_AGG(IF(client_category='Implementation', json_object,NULL) IGNORE NULLS)) AS product_implementation,
    TO_JSON(ARRAY_AGG(IF(client_category='Reporting', json_object,NULL) IGNORE NULLS)) AS reporting_detail

  FROM flattened f
  LEFT JOIN billing_agg b
    ON f.ID = b.ID
   AND f.Product_Association_ID = b.Product_Association_ID
  LEFT JOIN `playground-s-11-791f6efe.test.CUSTOMER_PRODUCT_ENTITLEMENT` CPE
    ON f.Product_Association_ID = CPE.sf_product_association_id

  GROUP BY
    f.business_unit_cd,
    f.ID,
    f.Product_Association_ID,
    f.CreatedDate,
    f.LastModifiedDate,
    CPE.customer_id
),

/* ============================================================
   SCD2 STREAM
   ============================================================ */

scd_source AS (
  SELECT 'EXPIRE' AS action_type, * FROM agg
  UNION ALL
  SELECT 'INSERT' AS action_type, * FROM agg
)

SELECT * FROM scd_source
) S

ON T.sf_product_configuration_id = S.sf_product_configuration_id
AND T.active_record_ind = 'Y'
AND S.action_type = 'EXPIRE'

WHEN MATCHED AND (
  TO_JSON_STRING(T.billing_detail) != TO_JSON_STRING(S.billing_detail)
)
THEN UPDATE SET
  active_record_ind = 'N',
  record_status_cd  = 'Inactive',
  record_end_dts    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED
AND S.action_type = 'INSERT'
THEN INSERT (
  sgk_customer_product_configuration_id,
  source_system_cd,
  sf_product_configuration_id,
  sf_product_association_id,
  business_unit_cd,
  product_level,
  client_type,
  billing_detail,
  program_rules,
  assigned_owner_contact_info,
  client_contract,
  account_management,
  ROI_performance_guarantees,
  product_implementation,
  reporting_detail,
  business_hash_key_id,
  record_hash_key_id,
  orig_src_pst_dts,
  source_last_process_dts,
  record_start_dts,
  record_end_dts,
  record_status_cd,
  active_record_ind,
  update_by
)
VALUES (
  GENERATE_UUID(),
  'PCT',
  S.sf_product_configuration_id,
  S.sf_product_association_id,
  S.business_unit_cd,
  S.product_level,
  S.client_type,
  S.billing_detail,
  S.program_rules,
  S.assigned_owner_contact_info,
  S.client_contract,
  S.account_management,
  S.ROI_performance_guarantees,
  S.product_implementation,
  S.reporting_detail,
  CONCAT(S.sf_product_configuration_id,'-',S.sf_product_association_id),
  TO_HEX(SHA256(S.sf_product_configuration_id)),
  S.CreatedDate,
  S.LastModifiedDate,
  CURRENT_TIMESTAMP(),
  TIMESTAMP('9999-12-31 00:00:00 UTC'),
  'Active',
  'Y',
  'PCT GCP Integration User'
);
