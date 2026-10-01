-- Same rule as customers: a Type 2 dimension may hold many versions, but
-- exactly one is current. Two current rows means a snapshot wrote a new version
-- without closing the old one, and anything filtering on is_current doubles.
--
-- Zero rows is a pass.

SELECT
    driver_id,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions

FROM {{ ref('dim_driver') }}

GROUP BY driver_id

HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
