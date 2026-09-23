-- One row per customer.
--
-- The SLA tier is derived, not given. customers.json carries an OTIF target
-- and sla_tiers.json carries three named tiers with their own targets, and
-- nothing joins the two. So the tier is read back from the target: 99.5 and
-- above is premium, 98 and above is standard, the rest is basic.
--
-- Flagging that because it is a guess dressed as a lookup. If the source ever
-- puts a tier on the customer record, this CASE goes away.

SELECT
    c.customer_id,
    c.customer_name,
    c.industry,
    c.contract_start_date,
    c.sla_otif_target_pct,
    c.sustainability_reporting_required,
    c.diesel_baseline_gco2_per_tonkm,

    CASE
        WHEN c.sla_otif_target_pct >= 99.5 THEN 'premium'
        WHEN c.sla_otif_target_pct >= 98.0 THEN 'standard'
        ELSE 'basic'
    END                                             AS sla_tier,

    t.sla_penalty_rate

FROM {{ ref('stg_einride_customers') }} c

LEFT JOIN {{ ref('stg_einride_sla_tiers') }} t
       ON CASE
              WHEN c.sla_otif_target_pct >= 99.5 THEN 'premium'
              WHEN c.sla_otif_target_pct >= 98.0 THEN 'standard'
              ELSE 'basic'
          END = t.sla_tier
