-- One row per vehicle: the last thing it said.
--
-- This is the "where is the fleet right now" table. It reads the OBT because
-- that is where every ping already has its context, and keeps only the newest
-- ping per vehicle.

SELECT
    vehicle_id,
    vehicle_type,
    oem,
    home_depot_id,
    event_time                                      AS last_seen_at,

    -- Charging wins over moving, because a truck can report a speed of zero
    -- with a route still assigned while it is plugged in at a depot.
    CASE
        WHEN is_charging              THEN 'Charging'
        WHEN speed_kmh > 0            THEN 'Driving'
        WHEN route_id IS NOT NULL     THEN 'Stopped on route'
        ELSE 'At depot'
    END                                             AS fleet_status,

    route_id,
    state_of_charge_pct,
    state_of_health_pct,
    latitude,
    longitude

FROM {{ ref('obt_telemetry_enriched') }}

-- Newest ping per vehicle. event_id breaks a tie if two pings share a second.
QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id
                           ORDER BY event_time DESC, event_id DESC) = 1
