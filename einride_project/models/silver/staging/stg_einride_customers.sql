SELECT
    customer_id,
    customer_name,
    industry,
    (CAST(contract_start_date AS DATE)) AS contract_start_date,
    sla_otif_target_pct,
    sustainability_reporting_required,
    diesel_baseline_gco2_per_tonkm
    
FROM {{source ('bronze', 'customers')}}