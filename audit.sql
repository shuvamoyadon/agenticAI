-- =========================================
-- STEP 1: CREATE SOURCE TEMP TABLE
-- =========================================
CREATE OR REPLACE TEMP TABLE SRC AS
SELECT
    GENERATE_UUID() AS sgk_customer_id,
    'PCT' AS source_system_cd,

    CASE
        WHEN business_unit_cd = 'Third_Party' THEN '3PY'
        WHEN business_unit_cd = 'Caremark' THEN 'PBM'
        WHEN business_unit_cd = 'Aetna' THEN 'HCB'
        ELSE business_unit_cd
    END AS business_unit_cd,

    CURRENT_TIMESTAMP() AS insert_dts,

    TO_HEX(MD5(CAST(customer_id AS STRING))) AS record_hash_key_id,

    CURRENT_TIMESTAMP() AS last_process_dts,
    COALESCE(business_effective_dt, DATE '1900-01-01') AS business_effective_dt,
    COALESCE(business_expiration_dt, DATE '9999-12-31') AS business_expiration_dt,

    CURRENT_TIMESTAMP() AS record_start_dts,
    TIMESTAMP('9999-12-31 00:00:00 UTC') AS record_end_dts,
    'Y' AS active_record_ind,

    indicator_flag,
    customer_id,
    customer_nm,
    sf_account_id,
    sf_platform_id_level_nm,

    *
FROM `edp-{{ env }}-storage.edp_ent_cma_plss_onboarding_src.T_CUSTOMER`
WHERE indicator_flag = 'N';

-- =========================================
-- STEP 2: VALIDATION LOGIC
-- =========================================
CREATE OR REPLACE TEMP TABLE VALIDATION_RESULT AS
SELECT
    S.*,

    -- NOT NULL CHECK
    CASE
        WHEN
            sgk_customer_id IS NULL OR
            source_system_cd IS NULL OR
            business_unit_cd IS NULL OR
            insert_dts IS NULL OR
            record_hash_key_id IS NULL OR
            business_effective_dt IS NULL OR
            business_expiration_dt IS NULL OR
            record_start_dts IS NULL OR
            record_end_dts IS NULL OR
            active_record_ind IS NULL OR
            customer_id IS NULL OR
            customer_nm IS NULL OR
            sf_account_id IS NULL
        THEN 'FAIL_NOT_NULL'
        ELSE 'PASS'
    END AS not_null_status,

    -- DATA TYPE CHECK
    CASE
        WHEN SAFE_CAST(customer_id AS STRING) IS NULL
        THEN 'FAIL_DATATYPE'
        ELSE 'PASS'
    END AS datatype_status,

    -- VALID VALUE CHECK
    CASE
        WHEN LOWER(sf_platform_id_level_nm) IN ('true','false')
        THEN 'PASS'
        ELSE 'FAIL_VALID_VALUE'
    END AS valid_value_status

FROM SRC S;

-- =========================================
-- STEP 3: DUPLICATE CHECK
-- =========================================
CREATE OR REPLACE TEMP TABLE DUP_CHECK AS
SELECT
    customer_id,
    customer_nm,
    sf_account_id,
    COUNT(*) AS cnt
FROM VALIDATION_RESULT
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

-- =========================================
-- STEP 4: SPLIT VALID / INVALID RECORDS
-- =========================================
CREATE OR REPLACE TEMP TABLE VALID_RECORDS AS
SELECT V.*
FROM VALIDATION_RESULT V
LEFT JOIN DUP_CHECK D
ON V.customer_id = D.customer_id
AND V.customer_nm = D.customer_nm
AND V.sf_account_id = D.sf_account_id
WHERE
    V.not_null_status = 'PASS'
    AND V.datatype_status = 'PASS'
    AND V.valid_value_status = 'PASS'
    AND D.customer_id IS NULL;

CREATE OR REPLACE TEMP TABLE INVALID_RECORDS AS
SELECT V.*
FROM VALIDATION_RESULT V
LEFT JOIN DUP_CHECK D
ON V.customer_id = D.customer_id
AND V.customer_nm = D.customer_nm
AND V.sf_account_id = D.sf_account_id
WHERE
    V.not_null_status != 'PASS'
    OR V.datatype_status != 'PASS'
    OR V.valid_value_status != 'PASS'
    OR D.customer_id IS NOT NULL;

-- =========================================
-- STEP 5: INSERT BAD RECORDS INTO AUDIT
-- =========================================
INSERT INTO `edp-{{ env }}-storage.edp_ent_cma_plss_onboarding_audit.AUDIT_BAD_CUSTOMER`
SELECT
    CURRENT_TIMESTAMP() AS audit_dts,
    *
FROM INVALID_RECORDS;

-- =========================================
-- STEP 6: MERGE USING ONLY VALID RECORDS
-- =========================================
MERGE INTO `edp-{{ env }}-storage.edp_ent_cma_plss_onboarding_cnf.CUSTOMER` T
USING VALID_RECORDS S
ON T.sf_account_id = S.sf_account_id
AND T.active_record_ind = 'Y'

WHEN MATCHED THEN
UPDATE SET
    T.active_record_ind = 'N',
    T.record_end_dts = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

-- INSERT NEW RECORDS
INSERT INTO `edp-{{ env }}-storage.edp_ent_cma_plss_onboarding_cnf.CUSTOMER`
SELECT *
FROM VALID_RECORDS;

-- =========================================
-- STEP 7: UPDATE SOURCE FLAG FOR VALID ONLY
-- =========================================
UPDATE `edp-{{ env }}-storage.edp_ent_cma_plss_onboarding_src.T_CUSTOMER` SRC
SET indicator_flag = 'Y'
FROM VALID_RECORDS V
WHERE SRC.sf_account_id = V.sf_account_id;
