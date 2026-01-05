## first modified merge. 

MERGE INTO `playground-s-11-53ad36ac.test.CUSTOMER_PRODUCT_CONFIGURATION` T
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
    JSON_QUERY_ARRAY(Payload) AS payload_array,
    cfg.Grouper_val as customer_population_group_id
  FROM `playground-s-11-53ad36ac.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION` cfg
  WHERE indicator_flag = 'N'
),

flattened AS (
  SELECT
   customer_population_group_id,
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
   PROGRAM RULE KEY/VALUE EXTRACTION
   ============================================================ */
program_rule_kv AS (
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
  WHERE client_category = 'Program Rules'
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
   PROGRAM RULE STRING AGGREGATION
   ============================================================ */

program_pairs AS (
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

  FROM program_rule_kv
  GROUP BY ID, Product_Association_ID
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
   PROGRAM RULE FINAL JSON (ARRAY OF 1 OBJECT)
   ============================================================ */

program_agg AS (
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
    ) AS program_rules
  FROM program_pairs
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
    f.customer_population_group_id,
    f.business_unit_cd,
    f.ID AS sf_product_configuration_id,
    f.Product_Association_ID AS sf_product_association_id,
    f.CreatedDate,
    f.LastModifiedDate,
    CPE.customer_id,

    TO_JSON(ARRAY_AGG(IF(client_category='Product Level', json_object,NULL) IGNORE NULLS)) AS product_level,
    TO_JSON(ARRAY_AGG(IF(client_category='Client Type', json_object,NULL) IGNORE NULLS)) AS client_type,
    ANY_VALUE(b.billing_detail) AS billing_detail,
    ANY_VALUE(pr.program_rules) AS program_rules,
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

   LEFT JOIN program_agg pr
    ON f.ID = pr.ID
   AND f.Product_Association_ID = pr.Product_Association_ID

  LEFT JOIN `playground-s-11-53ad36ac.test.CUSTOMER_PRODUCT_ENTITLEMENT` CPE
    ON f.Product_Association_ID = CPE.sf_product_association_id

  GROUP BY
    f.business_unit_cd,
    f.ID,
    f.Product_Association_ID,
    f.CreatedDate,
    f.LastModifiedDate,
    CPE.customer_id,
    f.customer_population_group_id
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
  customer_population_group_id,
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
  S.customer_population_group_id,
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


### then update 
UPDATE
  `edp-dev-storage.edp_ent_cma_plss_onboarding_src.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION` SRC
SET
  indicator_flag = 'Y'
FROM
  `edp-dev-storage.edp_ent_cma_plss_onboarding_cnf.CUSTOMER_PRODUCT_CONFIGURATION` TGT
WHERE
  SRC.ID = TGT.sf_product_configuration_id
  AND SRC.indicator_flag = 'N'
  AND TGT.active_record_ind = 'Y';


#### create the temp table to capture the active record if any grouper value match in target 

CREATE  TABLE `playground-s-11-53ad36ac.test.tmp_customer_product_configuration` AS
WITH src AS (
  SELECT
    cfg.Grouper_val AS customer_population_group_id,
    cfg1.Payload    AS payload
  FROM `playground-s-11-53ad36ac.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION` cfg
  JOIN `playground-s-11-53ad36ac.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION_1` cfg1
    ON cfg.Grouper_val = cfg1.Grouper_val
),

json_mapped AS (
  SELECT
    customer_population_group_id,
    JSON_QUERY(payload, '$[0].HEDISs__r')                AS HEDIS_client_to_metric,
    JSON_QUERY(payload, '$[0].Activity_Rules__r')        AS activity_rule,
    JSON_QUERY(payload, '$[0].Engagement_Rules__r')      AS engagement_rule,
    JSON_QUERY(payload, '$[0].Reactivation_Rules__r')    AS reactivation_rule,
    JSON_QUERY(payload, '$[0].CCNLs__r')                 AS care_connection_newsletter,
    JSON_QUERY(payload, '$[0].Fulfillments__r')          AS fulfillment,
    JSON_QUERY(payload, '$[0].Program_Flips__r')         AS program_flip_client_spinoff,
    JSON_QUERY(payload, '$[0].IVRs__r')                  AS interactive_voice_response,
    JSON_QUERY(payload, '$[0].Phone_Search__r')          AS phone_search,
    JSON_QUERY(payload, '$[0].Concurrent_Enrollment__r') AS concurrent_enrollment,
    JSON_QUERY(payload, '$[0].SMS_Campaigns__r')         AS sms_campaign,
    JSON_QUERY(payload, '$[0].Email_Campaigns__r')       AS email_campaign,
    JSON_QUERY(payload, '$[0].Health_Optimizers__r')     AS health_optimizer,
    JSON_QUERY(payload, '$[0].Call_Sheets__r')           AS call_sheet
  FROM src
),

t_active AS (
  SELECT *
  FROM `playground-s-11-53ad36ac.test.CUSTOMER_PRODUCT_CONFIGURATION`
  WHERE active_record_ind = 'Y'
)

SELECT
  GENERATE_UUID() AS sgk_customer_product_configuration_id,
  t.source_system_cd,
  t.business_unit_cd,
  t.line_of_business_cd,
  CURRENT_TIMESTAMP() AS insert_dts,
  t.source_record_sequence_nbr,
  t.business_hash_key_id,
  t.record_hash_key_id,
  CURRENT_TIMESTAMP() AS last_process_dts,
  t.orig_src_pst_dts,
  t.source_last_process_dts,
  t.business_effective_dt,
  t.business_expiration_dt,
  t.sgk_job_run_id,
  t.business_key_txt,
  t.customer_population_group_id,
  t.customer_id,
  t.sf_account_id,
  t.sf_group_census_id,
  t.sf_product_association_id,
  t.sf_product_configuration_id,
  t.sf_product_implementation_grouper_id,

  t.product_level,
  t.client_type,
  t.product_implementation,
  t.program_rules,
  t.assigned_owner_contact_info,
  t.client_contract,
  t.billing_detail,
  t.reporting_detail,
  t.ROI_performance_guarantees,
  t.account_management,

  j.HEDIS_client_to_metric,
  j.activity_rule,
  j.engagement_rule,
  j.reactivation_rule,
  j.care_connection_newsletter,
  j.fulfillment,
  j.program_flip_client_spinoff,
  j.interactive_voice_response,
  j.phone_search,
  j.concurrent_enrollment,
  j.sms_campaign,
  j.email_campaign,
  j.health_optimizer,
  j.call_sheet,

  'Active' AS record_status_cd,
  'Y'      AS active_record_ind,
  CURRENT_TIMESTAMP() AS record_start_dts,
  CAST(NULL AS TIMESTAMP) AS record_end_dts,
  'PCT GCP Integration User' AS update_by

FROM json_mapped j
JOIN t_active t
  ON t.customer_population_group_id = j.customer_population_group_id;


#### update the old record in target as any grouper value is present . 

UPDATE `playground-s-11-43525857.test.CUSTOMER_PRODUCT_CONFIGURATION`
SET
  active_record_ind = 'N',
  record_status_cd  = 'Inactive',
  record_end_dts    = CURRENT_TIMESTAMP()
WHERE active_record_ind = 'Y'
  AND customer_population_group_id IN (
    SELECT DISTINCT Grouper_val
    FROM `playground-s-11-43525857.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION`
  );


### final insert to insert remaining column along with other field value into target from temp table 


INSERT INTO `playground-s-11-53ad36ac.test.CUSTOMER_PRODUCT_CONFIGURATION`
(
  sgk_customer_product_configuration_id,
  source_system_cd,
  business_unit_cd,
  line_of_business_cd,
  insert_dts,
  source_record_sequence_nbr,
  business_hash_key_id,
  record_hash_key_id,
  last_process_dts,
  orig_src_pst_dts,
  source_last_process_dts,
  business_effective_dt,
  business_expiration_dt,
  sgk_job_run_id,
  business_key_txt,
  customer_population_group_id,
  customer_id,
  sf_account_id,
  sf_group_census_id,
  sf_product_association_id,
  sf_product_configuration_id,
  sf_product_implementation_grouper_id,

  product_level,
  client_type,
  product_implementation,
  program_rules,
  assigned_owner_contact_info,
  client_contract,
  billing_detail,
  reporting_detail,
  ROI_performance_guarantees,
  account_management,

  HEDIS_client_to_metric,
  activity_rule,
  engagement_rule,
  reactivation_rule,
  care_connection_newsletter,
  fulfillment,
  program_flip_client_spinoff,
  interactive_voice_response,
  phone_search,
  concurrent_enrollment,
  sms_campaign,
  email_campaign,
  health_optimizer,
  call_sheet,

  record_status_cd,
  active_record_ind,
  record_start_dts,
  record_end_dts,
  update_by
)
SELECT
  sgk_customer_product_configuration_id,
  source_system_cd,
  business_unit_cd,
  line_of_business_cd,
  insert_dts,
  source_record_sequence_nbr,
  business_hash_key_id,
  record_hash_key_id,
  last_process_dts,
  orig_src_pst_dts,
  source_last_process_dts,
  business_effective_dt,
  business_expiration_dt,
  sgk_job_run_id,
  business_key_txt,
  customer_population_group_id,
  customer_id,
  sf_account_id,
  sf_group_census_id,
  sf_product_association_id,
  sf_product_configuration_id,
  sf_product_implementation_grouper_id,

  product_level,
  client_type,
  product_implementation,
  program_rules,
  assigned_owner_contact_info,
  client_contract,
  billing_detail,
  reporting_detail,
  ROI_performance_guarantees,
  account_management,

  HEDIS_client_to_metric,
  activity_rule,
  engagement_rule,
  reactivation_rule,
  care_connection_newsletter,
  fulfillment,
  program_flip_client_spinoff,
  interactive_voice_response,
  phone_search,
  concurrent_enrollment,
  sms_campaign,
  email_campaign,
  health_optimizer,
  call_sheet,

  record_status_cd,
  active_record_ind,
  record_start_dts,
  record_end_dts,
  update_by
FROM  `playground-s-11-53ad36ac.test.tmp_customer_product_configuration`;





