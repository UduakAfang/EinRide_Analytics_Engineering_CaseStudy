{% macro sla_tier_from_target(otif_target_col) %}
    CASE
        WHEN {{ otif_target_col }} >= {{ var('sla_premium_min_otif_pct') }}  THEN 'premium'
        WHEN {{ otif_target_col }} >= {{ var('sla_standard_min_otif_pct') }} THEN 'standard'
        ELSE 'basic'
    END
{% endmacro %}
