-- One row per customer per version of their contract.
--
-- Type 2. A customer's OTIF target is what their deliveries are judged against,
-- and it changes at renewal -- so a delivery has to join to the target that was
-- in force on the day, not the one in force now.
--
-- That means customer_id is no longer unique here. Facts join on customer_key.
--
-- sla_tier_derived is named that way on purpose. The source has no tier column:
-- it gives an OTIF target, and sla_tiers.json gives three tiers with their own
-- targets, and nothing links them. The rule that bridges them is two numbers in
-- dbt_project.yml. A customer gets the tier they have reached, so 97.0 is basic
-- even though it is closer to standard's 98.0.
--
-- The penalty rate that follows is therefore inferred too. In a real contract a
-- penalty is negotiated, and two customers on 97% can owe different amounts.
-- Read it as a segmentation, not as money.

SELECT
    -- One row per contract version, so the key has to be too.
    {{ dbt_utils.generate_surrogate_key(['s.customer_id', 's.dbt_valid_from']) }}
                                                    AS customer_key,
    s.customer_id,

    s.customer_name,
    s.industry,
    CAST(s.contract_start_date AS DATE)             AS contract_start_date,

    s.sla_otif_target_pct,
    {{ sla_tier_from_target('s.sla_otif_target_pct') }}
                                                    AS sla_tier_derived,
    t.sla_penalty_rate                              AS sla_penalty_rate_derived,

    s.sustainability_reporting_required,
    s.diesel_baseline_gco2_per_tonkm,

    -- The window this version was true for. dbt writes both; valid_to is null
    -- on the version that is current.
    -- dbt stamps the first version with the day the snapshot first ran, which is
    -- after the history it describes. Backdate it so facts older than the first
    -- snapshot still find a row. Later versions keep their real dates.
    CASE WHEN LAG(s.dbt_valid_from) OVER (PARTITION BY s.customer_id ORDER BY s.dbt_valid_from) IS NULL
         THEN TIMESTAMP '1900-01-01' ELSE s.dbt_valid_from END
                                                    AS valid_from,
    s.dbt_valid_to                                  AS valid_to,

    -- Same fact as "valid_to is null", kept as a column because
    -- WHERE is_current is much harder to get wrong.
    s.dbt_valid_to IS NULL                          AS is_current

FROM {{ ref('snap_customers') }} s

LEFT JOIN {{ ref('stg_einride_sla_tiers') }} t
       ON {{ sla_tier_from_target('s.sla_otif_target_pct') }} = t.sla_tier
