
  SELECT
  CAST(id AS STRING) AS id,
  indicator_flag,

  -- Assigned Owner & Contact Information (ACI*)
  (
    SELECT
      IF(
        COUNT(*) = 0,
        JSON '{}',
        JSON_OBJECT(
          ARRAY_AGG(k),
          ARRAY_AGG(payload_json[k])
        )
      )
    FROM UNNEST(JSON_KEYS(payload_json)) AS k
    WHERE k LIKE 'ACI%'
      AND JSON_TYPE(payload_json[k]) != 'null'
  ) AS assigned_owner_contact_information,

  -- Accreditation & Audits (AcAu*)
  (
    SELECT
      IF(
        COUNT(*) = 0,
        JSON '{}',
        JSON_OBJECT(
          ARRAY_AGG(k),
          ARRAY_AGG(payload_json[k])
        )
      )
    FROM UNNEST(JSON_KEYS(payload_json)) AS k
    WHERE k LIKE 'AcAu%'
      AND JSON_TYPE(payload_json[k]) != 'null'
  ) AS accreditation_audits

FROM (
  SELECT
    id,
    indicator_flag,
    SAFE_CAST(payload AS JSON) AS payload_json
  FROM `playground-s-11-77e7acd7.test.src`
  WHERE indicator_flag = 'Y'
);

