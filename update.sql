UPDATE `playground-s-11-7fb7d994.test.CUSTOMER_ACCOUNT_STRUCTURE` tgt
SET
  -- LEVEL 1
  customer_account_structure_level_1_effective_dt =
    src.level_1_effective_dt,

  customer_account_structure_level_1_expiration_dt =
    COALESCE(src.level_1_expiration_dt, DATE '9999-12-31'),

  -- LEVEL 2
  customer_account_structure_level_2_effective_dt =
    src.level_2_effective_dt,

  customer_account_structure_level_2_expiration_dt =
    COALESCE(src.level_2_expiration_dt, DATE '9999-12-31'),

  -- LEVEL 3
  customer_account_structure_level_3_effective_dt =
    src.level_3_effective_dt,

  customer_account_structure_level_3_expiration_dt =
    COALESCE(src.level_3_expiration_dt, DATE '9999-12-31'),

  -- LEVEL 4
  customer_account_structure_level_4_effective_dt =
    src.level_4_effective_dt,

  customer_account_structure_level_4_expiration_dt =
    COALESCE(src.level_4_expiration_dt, DATE '9999-12-31')

FROM
(
  SELECT
    sf_account_id,

    -- identify max level dynamically
    MAX(level_num) AS max_level,

    -- LEVEL 1
    MAX(IF(level_num = 1, business_effective_dt, NULL)) AS level_1_effective_dt,
    MAX(IF(level_num = 1, business_expiration_dt, NULL)) AS level_1_expiration_dt,

    -- LEVEL 2
    MAX(IF(level_num = 2, business_effective_dt, NULL)) AS level_2_effective_dt,
    MAX(IF(level_num = 2, business_expiration_dt, NULL)) AS level_2_expiration_dt,

    -- LEVEL 3
    MAX(IF(level_num = 3, business_effective_dt, NULL)) AS level_3_effective_dt,
    MAX(IF(level_num = 3, business_expiration_dt, NULL)) AS level_3_expiration_dt,

    -- LEVEL 4
    MAX(IF(level_num = 4, business_effective_dt, NULL)) AS level_4_effective_dt,
    MAX(IF(level_num = 4, business_expiration_dt, NULL)) AS level_4_expiration_dt

  FROM
  (
    SELECT
      sf_account_id,
      business_effective_dt,
      business_expiration_dt,
      SAFE_CAST(REGEXP_EXTRACT(sf_client_level, r'(\d+)') AS INT64) AS level_num
    FROM `playground-s-11-7fb7d994.test.CUSTOMER_ACCOUNT_STRUCTURE`
  )
  GROUP BY sf_account_id
) src

WHERE
  tgt.sf_account_id = src.sf_account_id
  AND SAFE_CAST(REGEXP_EXTRACT(tgt.sf_client_level, r'(\d+)') AS INT64) = src.max_level;
