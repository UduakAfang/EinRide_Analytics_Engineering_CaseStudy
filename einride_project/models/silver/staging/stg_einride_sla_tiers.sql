SELECT
    key                         AS sla_tier,
    sla_tiers.otif_target       AS sla_otif_target_pct,
    sla_tiers.penalty_rate      AS sla_penalty_rate

FROM {{source ('bronze', 'sla_tiers')}}
