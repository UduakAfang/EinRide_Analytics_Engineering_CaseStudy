-- One row per depot. Six of them, and it still earns a table: region doubles
-- as the electricity price zone, so this is what ties a vehicle to what its
-- power costs.

SELECT
    depot_id,
    depot_name,
    region,
    region                                          AS price_zone,
    latitude,
    longitude,
    charger_count,
    max_charger_power_kw

FROM {{ ref('stg_einride_depots') }}
