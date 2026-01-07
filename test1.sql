
CREATE TEMP TABLE SRC AS
SELECT
  GENERATE_UUID() AS sgk_customer_account_structure_id,
  CAS.source_system_cd,
  CASE
    WHEN CAS.business_unit_cd = 'Third_Party' THEN '3PY'
    WHEN CAS.business_unit_cd = 'Caremark' THEN 'PBM'
    WHEN CAS.business_unit_cd = 'Aetna' THEN 'HCB'
    ELSE CAS.business_unit_cd
  END AS business_unit_cd,
  CAS.line_of_business_cd,
  CURRENT_TIMESTAMP() AS insert_dts,
  NULL AS record_status_cd,
  NULL AS source_record_sequence_nbr,

  TO_HEX(MD5(
    ARRAY_TO_STRING([
      COALESCE(CAST(CAS.customer_id AS STRING), 'X'),
      COALESCE(CAST(CAS.business_unit_cd AS STRING), 'X'),
      COALESCE(CAST(CAS.line_of_business_cd AS STRING), 'X')
    ], '|')
  )) AS business_hash_key_id,

  TO_HEX(MD5(
    ARRAY_TO_STRING([
      COALESCE(CAST(CAS.customer_id AS STRING), 'X'),
      COALESCE(CAST(CAS.plan_sponsor_unique_id AS STRING), 'X'),
      COALESCE(CAST(CAS.sf_group_census_id AS STRING), 'X')
    ], '|')
  )) AS business_key_txt,

  TO_HEX(MD5(
    ARRAY_TO_STRING([
      CAST(CAS.source_system_cd AS STRING),
      CAST(CAS.business_unit_cd AS STRING),
      CAST(CAS.line_of_business_cd AS STRING),
      CAST(CAS.plan_sponsor_unique_id AS STRING),
      CAST(CAS.account_status_cd AS STRING),
      CAST(CAS.sf_group_census_id AS STRING)
    ], '|')
  )) AS record_hash_key_id,

  CURRENT_TIMESTAMP() AS last_process_dts,
  CAST(CAS.source_last_process_dts AS TIMESTAMP) AS source_last_process_dts,
  COALESCE(CAS.business_effective_dt, DATE '1900-01-01') AS business_effective_dt,
  COALESCE(CAS.business_expiration_dt, DATE '9999-12-31') AS business_expiration_dt,
  CURRENT_TIMESTAMP() AS record_start_dts,
  TIMESTAMP('9999-12-31 00:00:00 UTC') AS record_end_dts,
  'Y' AS active_record_ind,

  CAS.sgk_job_run_id,
  CAS.sgk_customer_id,
  CAS.plan_sponsor_unique_id,
  CAS.plan_sponsor_unique_nm,
  CAS.account_employment_status_cd,
  CAS.cms_contract_st_cd,
  CAS.cms_lep_attestation_cd,
  CAS.cobra_ind,
  CAS.contact_typ_cd,
  CAS.cust_mkt_subseg_cd,
  CAS.customer_category_cd,
  CAS.customer_group_contact_nm,
  CAS.customer_group_nm,
  CAS.decision_maker_typ_cd,
  CAS.eligible_employee_cnt,
  CAS.emplyr_class_cd,
  CAS.excld_cd,
  CAS.exprnc_acctng_basis_cd,
  CAS.group_size_cd,
  CAS.renewal_effective_dt,
  CAS.renewal_expiration_dt,
  CAS.service_fld_office_cd,
  CAS.sic_cd,
  CAS.small_grp_reform_ind,
  CAS.source_pln_typ_cd,
  CAS.source_pln_typ_id,
  CAS.stplss_st_cd,
  CAS.total_employee_cnt,
  CAS.trust_cd,
  CAS.trust_nm,
  CAS.account_id,
  CAS.account_status_cd,
  CAS.data_share_consent_ind,
  CAS.client_plan_group_id,
  CAS.rx_claim_client_cd,
  CAS.non_business_unit_customer_ind,
  CAS.non_business_unit_customer_type_cd,
  CAS.non_business_unit_line_of_business_cd,
  CAS.cfo_cd,
  CAS.clm_office_cd,
  CAS.super_client_effective_dt,
  CAS.super_client_expiration_dt,
  CAS.quantum_leap_client_cd,
  CAS.quantum_leap_client_id,
  CAS.customer_account_structure_level_1_id,
  CAS.customer_account_structure_level_2_id,
  CAS.customer_account_structure_level_3_id,
  CAS.customer_account_structure_level_4_id,
  CAS.customer_account_structure_level_1_nm,
  CAS.customer_account_structure_level_2_nm,
  CAS.customer_account_structure_level_3_nm,
  CAS.customer_account_structure_level_4_nm,
  CAS.customer_account_type_cd,
  CAS.rdc_ind,
  CAS.rdc_level_cd,
  CAS.sf_group_census_id,
  CAS.sf_coalition_nm,
  CAS.sf_member_list_typ_nm,
  CAS.orig_src_pst_dts,
  CAS.sf_account_id,
  CUST.customer_id
FROM edp-dev-storage.edp_ent_cma_plss_onboarding_src.T_CUSTOMER_ACCOUNT_STRUCTURE CAS
LEFT JOIN edp-dev-storage.edp_ent_cma_plss_onboarding_src.T_CUSTOMER CUST
  ON CAS.sf_account_id = CUST.sf_account_id;

-- =====================================================
-- STEP 1: Expire existing active records (SCD2 close)
-- =====================================================
MERGE INTO edp-dev-storage.edp_ent_cma_plss_onboarding_cnf.CUSTOMER_ACCOUNT_STRUCTURE T
USING SRC S
ON T.sf_group_census_id = S.sf_group_census_id
AND T.active_record_ind = 'Y'

WHEN MATCHED
AND T.record_hash_key_id <> S.record_hash_key_id
THEN UPDATE SET
  T.active_record_ind = 'N',
  T.record_end_dts = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

-- =====================================================
-- STEP 2: Insert new records (new + changed)
-- =====================================================
INSERT INTO edp-dev-storage.edp_ent_cma_plss_onboarding_cnf.CUSTOMER_ACCOUNT_STRUCTURE
SELECT
  S.*
FROM SRC S
LEFT JOIN edp-dev-storage.edp_ent_cma_plss_onboarding_cnf.CUSTOMER_ACCOUNT_STRUCTURE T
  ON T.sf_group_census_id = S.sf_group_census_id
  AND T.active_record_ind = 'Y'
WHERE T.sf_group_census_id IS NULL;
