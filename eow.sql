MERGE INTO `playground-s-11-d5e75d75.test.CUSTOMER_PRODUCT_CONFIGURATION` T
USING (

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
  FROM `playground-s-11-d5e75d75.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION`
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

product_level_kv AS (
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
  WHERE client_category = 'Product Level'
),

client_type_kv AS (
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
  WHERE client_category = 'Client Type'
),

assigned_owner_contact_kv AS (
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
  WHERE client_category = 'Assigned Owner & Contact Information'
),

client_contract_kv AS (
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
  WHERE client_category = 'Client Contract'
),

account_management_kv AS (
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
  WHERE client_category = 'Account Management'
),

roi_pgs_kv AS (
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
  WHERE client_category = 'ROI/PGs'
),

implementation_kv AS (
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
  WHERE client_category = 'Implementation'
),

reporting_kv AS (
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
  WHERE client_category = 'Reporting'
),

program_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS program_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM program_rule_kv
  GROUP BY ID, Product_Association_ID
),

billing_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM billing_kv
  GROUP BY ID, Product_Association_ID
),

product_level_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS product_level_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM product_level_kv
  GROUP BY ID, Product_Association_ID
),

client_type_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS client_type_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM client_type_kv
  GROUP BY ID, Product_Association_ID
),

assigned_owner_contact_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS assigned_owner_contact_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM assigned_owner_contact_kv
  GROUP BY ID, Product_Association_ID
),

client_contract_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS client_contract_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM client_contract_kv
  GROUP BY ID, Product_Association_ID
),

account_management_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS account_management_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM account_management_kv
  GROUP BY ID, Product_Association_ID
),

roi_pgs_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS roi_pgs_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM roi_pgs_kv
  GROUP BY ID, Product_Association_ID
),

implementation_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS implementation_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM implementation_kv
  GROUP BY ID, Product_Association_ID
),

reporting_pairs AS (
  SELECT
    ID,
    Product_Association_ID,
    STRING_AGG(
  CASE
    WHEN key_name = 'Contracted_LOB_Sub_Category' AND raw_val LIKE '%;%' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      TO_JSON_STRING(ARRAY(SELECT REGEXP_REPLACE(TRIM(v), r' +', '_') FROM UNNEST(SPLIT(raw_val, ';')) v WHERE TRIM(v) <> ''))
    )
    WHEN key_name = 'Contracted_LOB_Sub_Category' THEN CONCAT(
      '"Contracted_LOB_Sub_Category":',
      IF(raw_val IS NULL, 'null', TO_JSON_STRING(REGEXP_REPLACE(TRIM(raw_val), r' +', '_')))
    )
    ELSE CONCAT('"', key_name, '":', IF(raw_val IS NULL, 'null', TO_JSON_STRING(raw_val)))
  END,
  ',') AS reporting_kv_string,
    MAX(JSON_VALUE(json_object,'$.Client_External_Id__c')) AS Client_External_Id__c,
    MAX(JSON_VALUE(json_object,'$.Configured_Product__c')) AS Configured_Product__c,
    MAX(JSON_VALUE(json_object,'$.Grouper_ID__c')) AS Grouper_ID__c,
    MAX(JSON_VALUE(json_object,'$.line_of_business')) AS line_of_business,
    MAX(JSON_VALUE(json_object,'$.LastModifiedBy.Name.value')) AS modified_by
  FROM reporting_kv
  GROUP BY ID, Product_Association_ID
),

program_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', program_kv_string, ',"Attribute_Category__c":"Program Rules",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS program_rules
  FROM program_pairs
),

billing_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', kv_string, ',"Attribute_Category__c":"Billing",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS billing_detail
  FROM billing_pairs
),

product_level_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', product_level_kv_string, ',"Attribute_Category__c":"Product Level",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS product_level
  FROM product_level_pairs
),

client_type_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', client_type_kv_string, ',"Attribute_Category__c":"Client Type",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS client_type
  FROM client_type_pairs
),

assigned_owner_contact_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', assigned_owner_contact_kv_string, ',"Attribute_Category__c":"Assigned Owner & Contact Information",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS assigned_owner_contact_info
  FROM assigned_owner_contact_pairs
),

client_contract_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', client_contract_kv_string, ',"Attribute_Category__c":"Client Contract",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS client_contract
  FROM client_contract_pairs
),

account_management_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', account_management_kv_string, ',"Attribute_Category__c":"Account Management",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS account_management
  FROM account_management_pairs
),

roi_pgs_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', roi_pgs_kv_string, ',"Attribute_Category__c":"ROI/PGs",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS ROI_performance_guarantees
  FROM roi_pgs_pairs
),

implementation_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', implementation_kv_string, ',"Attribute_Category__c":"Implementation",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS product_implementation
  FROM implementation_pairs
),

reporting_agg AS (
  SELECT
    ID,
    Product_Association_ID,
    TO_JSON(ARRAY(SELECT PARSE_JSON(CONCAT('{', reporting_kv_string, ',"Attribute_Category__c":"Reporting",',
    '"Client_External_Id__c":"',Client_External_Id__c,'",',
    '"Configured_Product__c":"',Configured_Product__c,'",',
    '"Grouper_ID__c":"',Grouper_ID__c,'",',
    '"Product_Association_ID":"',Product_Association_ID,'",',
    '"line_of_business":"',line_of_business,'",',
    '"LastModifiedBy":{"Name":{"Value":"',modified_by,'"}}}')))) AS reporting_detail
  FROM reporting_pairs
),

agg AS (
  SELECT
    f.business_unit_cd,
    f.ID AS sf_product_configuration_id,
    f.Product_Association_ID AS sf_product_association_id,
    f.CreatedDate,
    f.LastModifiedDate,
    CPE.customer_id,
    ANY_VALUE(pl.product_level) AS product_level,
    ANY_VALUE(ct.client_type) AS client_type,
    ANY_VALUE(b.billing_detail) AS billing_detail,
    ANY_VALUE(pr.program_rules) AS program_rules,
    ANY_VALUE(ao.assigned_owner_contact_info) AS assigned_owner_contact_info,
    ANY_VALUE(cc.client_contract) AS client_contract,
    ANY_VALUE(am.account_management) AS account_management,
    ANY_VALUE(rp.ROI_performance_guarantees) AS ROI_performance_guarantees,
    ANY_VALUE(impl.product_implementation) AS product_implementation,
    ANY_VALUE(rep.reporting_detail) AS reporting_detail
  FROM flattened f
  LEFT JOIN billing_agg b
    ON f.ID = b.ID AND f.Product_Association_ID = b.Product_Association_ID
  LEFT JOIN program_agg pr
    ON f.ID = pr.ID AND f.Product_Association_ID = pr.Product_Association_ID
  LEFT JOIN product_level_agg pl
    ON f.ID = pl.ID AND f.Product_Association_ID = pl.Product_Association_ID
  LEFT JOIN client_type_agg ct
    ON f.ID = ct.ID AND f.Product_Association_ID = ct.Product_Association_ID
  LEFT JOIN assigned_owner_contact_agg ao
    ON f.ID = ao.ID AND f.Product_Association_ID = ao.Product_Association_ID
  LEFT JOIN client_contract_agg cc
    ON f.ID = cc.ID AND f.Product_Association_ID = cc.Product_Association_ID
  LEFT JOIN account_management_agg am
    ON f.ID = am.ID AND f.Product_Association_ID = am.Product_Association_ID
  LEFT JOIN roi_pgs_agg rp
    ON f.ID = rp.ID AND f.Product_Association_ID = rp.Product_Association_ID
  LEFT JOIN implementation_agg impl
    ON f.ID = impl.ID AND f.Product_Association_ID = impl.Product_Association_ID
  LEFT JOIN reporting_agg rep
    ON f.ID = rep.ID AND f.Product_Association_ID = rep.Product_Association_ID
  LEFT JOIN `playground-s-11-d5e75d75.test.CUSTOMER_PRODUCT_ENTITLEMENT` CPE
    ON f.Product_Association_ID = CPE.sf_product_association_id
  GROUP BY
    f.business_unit_cd,
    f.ID,
    f.Product_Association_ID,
    f.CreatedDate,
    f.LastModifiedDate,
    CPE.customer_id
),

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
