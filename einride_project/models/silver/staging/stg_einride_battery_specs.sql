SELECT
    key                                         AS battery_chemistry,
    battery_chemistries.degradation_rate        AS degradation_rate_per_cycle,
    battery_chemistries.cycle_life              AS cycle_life,
    battery_chemistries.max_charge_rate_c       AS max_charge_rate_c

FROM {{source ('bronze', 'battery_specs')}}
