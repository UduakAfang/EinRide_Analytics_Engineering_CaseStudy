SELECT
    key                                     AS charger_type,
    charger_types.power_kw                  AS charger_power_kw,
    charger_types.connector                 AS connector_type,
    charger_types.typical_duration_hours    AS typical_charge_duration_hours

FROM {{source ('bronze', 'charger_types')}}
