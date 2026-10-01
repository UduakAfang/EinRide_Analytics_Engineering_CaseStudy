-- A Type 2 dimension may hold many versions of a customer, but exactly one of
-- them is current. Two current rows means a snapshot ran without closing the
-- old version, and every join that filters on is_current would then double.
--
-- Zero rows is a pass.

SELECT
    customer_id,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions

FROM {{ ref('dim_customer') }}

GROUP BY customer_id

HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
