
MERGE INTO `playground-s-11-443f35ef.test.CUSTOMER_PRODUCT_CONFIGURATIONS` T
USING (
  -- ==========================================
  -- SOURCE DATA 
  -- ==========================================
  WITH exploded AS (
    SELECT
      line_of_business AS business_unit_cd,
      ID,
      Product_Association_ID,
      JSON_QUERY_ARRAY(Payload) AS payload_array
    FROM `playground-s-11-443f35ef.test.T_PCT_GCP_FIN_PRODUCT_SRC`
    WHERE active_indicator_flag = 'Y'
  ),

  flattened AS (
    SELECT
      business_unit_cd,
      ID,
      Product_Association_ID,
      REGEXP_REPLACE(
        JSON_VALUE(element, '$.Attribute_Category__c'),
        r'\\u0026amp;|&amp;',
        '&'
      ) AS client_category,
      element AS json_object
    FROM exploded
    CROSS JOIN UNNEST(payload_array) AS element
  ),

  agg AS (
    SELECT
      business_unit_cd,
      ID AS sf_product_configuration_id,
      Product_Association_ID AS sf_product_association_id,

      TO_JSON(ARRAY_AGG(IF(client_category = 'Product Level', json_object, NULL) IGNORE NULLS)) AS product_level,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Client Type', json_object, NULL) IGNORE NULLS)) AS client_type,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Billing', json_object, NULL) IGNORE NULLS)) AS billing_detail,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Program Rules', json_object, NULL) IGNORE NULLS)) AS program_rules,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Assigned Owner & Contact Information', json_object, NULL) IGNORE NULLS)) AS assigned_owner_contact_info,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Client Contract', json_object, NULL) IGNORE NULLS)) AS client_contract,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Account Management', json_object, NULL) IGNORE NULLS)) AS account_management,
      TO_JSON(ARRAY_AGG(IF(client_category = 'ROI/PGs', json_object, NULL) IGNORE NULLS)) AS ROI_performance_guarantees,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Implementation', json_object, NULL) IGNORE NULLS)) AS product_implementation,
      TO_JSON(ARRAY_AGG(IF(client_category = 'Reporting', json_object, NULL) IGNORE NULLS)) AS reporting_detail
    FROM flattened
    GROUP BY
      business_unit_cd,
      ID,
      Product_Association_ID
  ),

  -- ==========================================
  -- DUPLICATE STREAM FOR INSERT
  -- ==========================================
  scd_source AS (
    SELECT
      'EXPIRE' AS action_type,
      *
    FROM agg

    UNION ALL

    SELECT
      'INSERT' AS action_type,
      *
    FROM agg
  )

  SELECT * FROM scd_source
) S

ON T.sf_product_configuration_id = S.sf_product_configuration_id
AND T.active_record_ind = 'Y'
AND S.action_type = 'EXPIRE'

-- EXPIRE CURRENT ACTIVE RECORD
WHEN MATCHED
AND (
  TO_JSON_STRING(T.product_level) != TO_JSON_STRING(S.product_level)
  OR TO_JSON_STRING(T.client_type) != TO_JSON_STRING(S.client_type)
  OR TO_JSON_STRING(T.billing_detail) != TO_JSON_STRING(S.billing_detail)
  OR TO_JSON_STRING(T.program_rules) != TO_JSON_STRING(S.program_rules)
  OR TO_JSON_STRING(T.assigned_owner_contact_info) != TO_JSON_STRING(S.assigned_owner_contact_info)
  OR TO_JSON_STRING(T.client_contract) != TO_JSON_STRING(S.client_contract)
  OR TO_JSON_STRING(T.account_management) != TO_JSON_STRING(S.account_management)
  OR TO_JSON_STRING(T.ROI_performance_guarantees) != TO_JSON_STRING(S.ROI_performance_guarantees)
  OR TO_JSON_STRING(T.product_implementation) != TO_JSON_STRING(S.product_implementation)
  OR TO_JSON_STRING(T.reporting_detail) != TO_JSON_STRING(S.reporting_detail)
)
THEN
  UPDATE SET
    T.active_record_ind = 'N',
    T.updated_timestamp = CURRENT_TIMESTAMP()

-- INSERT NEW VERSION
WHEN NOT MATCHED
AND S.action_type = 'INSERT'
THEN
  INSERT (
    sgk_customer_product_configuration_id,
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
    active_record_ind,
    created_timestamp,
    updated_timestamp
  )
  VALUES (
    GENERATE_UUID(),
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
    'Y',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );



UPDATE `playground-s-11-443f35ef.test.T_PCT_GCP_FIN_PRODUCT_SRC` SRC
SET active_indicator_flag = 'N'
FROM `playground-s-11-443f35ef.test.CUSTOMER_PRODUCT_CONFIGURATIONS` TGT
WHERE SRC.ID = TGT.sf_product_configuration_id
and src.active_indicator_flag = 'Y' and TGT.active_record_ind='Y';
