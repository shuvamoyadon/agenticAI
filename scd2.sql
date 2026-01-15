### step 1 

CREATE TABLE `playground-s-11-77e7acd7.test.src` (
  ID STRING NOT NULL,
  AccountId STRING,
  line_of_business STRING,
  CreatedDate TIMESTAMP,
  LastModifiedDate TIMESTAMP,
  inserted_time TIMESTAMP,
  updated_time TIMESTAMP,
  indicator_flag STRING,
  Payload JSON
);

### step 2

INSERT INTO `playground-s-11-a9c9b9af.test.src` (
  ID,
  AccountId,
  line_of_business,
  CreatedDate,
  LastModifiedDate,
  inserted_time,
  updated_time,
  indicator_flag,
  Payload
)
SELECT
  'SRC-001' AS ID,
  '001bc00000Smc8AAR' AS AccountId,
  'Healthcare' AS line_of_business,
  CURRENT_TIMESTAMP() AS CreatedDate,
  CURRENT_TIMESTAMP() AS LastModifiedDate,
  CURRENT_TIMESTAMP() AS inserted_time,
  CURRENT_TIMESTAMP() AS updated_time,
  'Y' AS indicator_flag,

  TO_JSON(
    ARRAY[
      STRUCT(
        'AMA' AS ACI_Client_Audit_Contact__c,
        NULL AS ACI_Client_Primary_Claim_Denial_Contact__c,
        NULL AS ACI_Client_QA_Contact__c,
        NULL AS ACI_Privacy_Billing_Contact__c,
        NULL AS ACI_Secondary_Claim_Denial_Contact__c,
        NULL AS AcAu_Client_Audit_Tools_List__c,
        NULL AS AcAu_Client_Delegation_Reports__c,
        NULL AS AcAu_Client_List__c,
        NULL AS AcAu_Client_NCQA_Usage_Flag__c,
        NULL AS AcAu_Client_Oversight_Plan__c,
        NULL AS AcAu_LOB_Specific_Reqs__c,
        NULL AS AcAu_NCQA__c,
        NULL AS AcAu_NCQA_Pls_Specify__c,
        '001bc00000Smc8AAR' AS AccountId,
        NULL AS Billing_According_Claims_Auto_Adjudicate__c,
        NULL AS Billing_Bimonthly_Filing_Titles__c,
        NULL AS Billing_Bypass_Add_Cred__c,
        NULL AS Billing_Claims_Adjudication_Details__c,
        NULL AS Billing_Cost_to_Client__c,
        NULL AS Billing_Credentialing_Req_Details__c,
        NULL AS Billing_Entity__c,
        NULL AS Billing_Fee_Schedule_Details__c,
        NULL AS Billing_File_Transfer_Frequency__c,
        NULL AS Billing_Interactive_Billable_Rare__c,
        NULL AS Billing_Is_CBB_Applicable__c,
        NULL AS Billing_List_Billable_Member_Type__c,
        'No' AS Billing_List_Pricing_Flat__c,
        NULL AS Billing_Load_Accordant_Fee__c,
        'Yes' AS Billing_Managed_Status__c,
        NULL AS Billing_Mgmt_Complete_Status__c,
        NULL AS Billing_Oncology_Members__c,
        NULL AS Billing_Please_Specify_CBB__c,
        NULL AS Billing_Please_Specify_EOB__c
      )
    ]
  ) AS Payload;


#### step 3 . create a table to capture active records for matching id : 

CREATE or replace TABLE `playground-s-11-77e7acd7.test.src_prepared` AS
WITH base AS (
  SELECT
    id,
    indicator_flag,
    IFNULL(JSON_QUERY_ARRAY(payload)[SAFE_OFFSET(0)], JSON '{}') AS payload_obj
  FROM `playground-s-11-a9c9b9af.test.src`
  WHERE indicator_flag = 'Y'
)

SELECT
  CAST(id AS STRING) AS id,
  indicator_flag,

  -- Extract line_of_business from payload
  JSON_VALUE(payload_obj, '$.line_of_business') AS line_of_business,

  -- Assigned Owner & Contact Information (ACI*)
  (
    SELECT
      IF(
        COUNT(*) = 0,
        JSON '{}',
        JSON_OBJECT(ARRAY_AGG(k), ARRAY_AGG(payload_obj[k]))
      )
    FROM UNNEST(JSON_KEYS(payload_obj)) AS k
    WHERE k LIKE 'ACI%'
      AND JSON_TYPE(payload_obj[k]) != 'null'
  ) AS assigned_owner_contact_information,

  -- Accreditation & Audits (AcAu*)
  (
    SELECT
      IF(
        COUNT(*) = 0,
        JSON '{}',
        JSON_OBJECT(ARRAY_AGG(k), ARRAY_AGG(payload_obj[k]))
      )
    FROM UNNEST(JSON_KEYS(payload_obj)) AS k
    WHERE k LIKE 'AcAu%'
      AND JSON_TYPE(payload_obj[k]) != 'null'
  ) AS accreditation_audits

FROM base;



### step 4: I have created target table with few cilumn and audit columns

CREATE TABLE IF NOT EXISTS `playground-s-11-77e7acd7.test.customer_contacts_scd` (
  surrogate_key STRING NOT NULL,
  id STRING NOT NULL,

  assigned_owner_contact_information JSON,
  accreditation_audits JSON,

  indicator_flag STRING, 
  active_flg STRING,

  effective_start_ts TIMESTAMP,
  effective_end_ts TIMESTAMP,
  load_ts TIMESTAMP
)
PARTITION BY DATE(load_ts)
CLUSTER BY id;


#### step 5 : now check the matching ID recotd to deactivate the records 

UPDATE `playground-s-11-77e7acd7.test.customer_contacts_scd` tgt
SET
  active_flg = 'N',
  effective_end_ts = CURRENT_TIMESTAMP(),
  load_ts = CURRENT_TIMESTAMP()
WHERE tgt.active_flg = 'Y'
  AND EXISTS (
    SELECT 1
    FROM `playground-s-11-77e7acd7.test.src_prepared` src
    WHERE src.id = tgt.id
      AND (
        TO_JSON_STRING(IFNULL(tgt.assigned_owner_contact_information, JSON '{}')) !=
        TO_JSON_STRING(IFNULL(src.assigned_owner_contact_information, JSON '{}'))
        OR
        TO_JSON_STRING(IFNULL(tgt.accreditation_audits, JSON '{}')) !=
        TO_JSON_STRING(IFNULL(src.accreditation_audits, JSON '{}'))
      )
  );


### Add another row for matching id or non matching .. all will go to insert 

INSERT INTO `playground-s-11-77e7acd7.test.customer_contacts_scd` (
  surrogate_key,
  id,
  assigned_owner_contact_information,
  accreditation_audits,
  indicator_flag,
  active_flg,
  effective_start_ts,
  effective_end_ts,
  load_ts
)
SELECT
  GENERATE_UUID(),
  src.id,
  src.assigned_owner_contact_information,
  src.accreditation_audits,
  src.indicator_flag,
  'Y' AS active_flg,
  CURRENT_TIMESTAMP(),
  TIMESTAMP('9999-12-31'),
  CURRENT_TIMESTAMP()
FROM `playground-s-11-77e7acd7.test.src_prepared`  src
LEFT JOIN `playground-s-11-77e7acd7.test.customer_contacts_scd` tgt
  ON tgt.id = src.id
 AND tgt.active_flg = 'Y'
WHERE
  tgt.id IS NULL
  OR (
    TO_JSON_STRING(IFNULL(tgt.assigned_owner_contact_information, JSON '{}')) !=
    TO_JSON_STRING(IFNULL(src.assigned_owner_contact_information, JSON '{}'))
    OR
    TO_JSON_STRING(IFNULL(tgt.accreditation_audits, JSON '{}')) !=
    TO_JSON_STRING(IFNULL(src.accreditation_audits, JSON '{}'))
  );

#### update source table to change the indicator flag to N

UPDATE  `playground-s-11-77e7acd7.test.src` src2
SET indicator_flag = 'N'
WHERE indicator_flag = 'Y'
  AND EXISTS (
    SELECT 1
    FROM `playground-s-11-77e7acd7.test.customer_contacts_scd` tgt
    WHERE tgt.id = src2.id
      AND tgt.active_flg = 'Y'
  );



