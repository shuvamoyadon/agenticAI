UPDATE edp-dev-storage.edp_ent_cma_plss_onboarding_src.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION
SET
  payload = (
    SELECT ARRAY_AGG(
      CASE
        WHEN JSON_VALUE(elem, '$.Id') = 'a09bc00000CCJOSAA5'
        THEN JSON_SET(
               elem,
               '$.AttributeName__c', 'NEW_ATTRIBUTE_VALUE'
             )
        ELSE elem
      END
    )
    FROM UNNEST(payload) AS elem
  ),
  indicator_flag = 'Y'
WHERE ID = 'a01bc00000UuQWOAA3'
  AND indicator_flag = 'N';
