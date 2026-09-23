-- One row per trip.
--
-- A trip is a run of rows for one vehicle on the same route_id. Not
-- IGNITION_ON to IGNITION_OFF, because a truck also shuts down for the 45
-- minute break and for driver changes.
--
-- Open trips are left out. If nothing came after it, I can't say it finished.

WITH marked AS (

    SELECT
        vehicle_id,
        event_time,
        trigger_type,
        route_id,
        driver_id,
        shipment_id,
        customer_id,
        speed_kmh,
        state_of_charge_pct,
        energy_meter_kwh,
        distance_meter_km,

        -- New trip when the route changes, including from null. Partitioned by
        -- vehicle because 120 of them report interleaved.
        CASE
            WHEN route_id IS NOT NULL
             AND route_id IS DISTINCT FROM LAG(route_id) OVER (
                    PARTITION BY vehicle_id ORDER BY event_time)
            THEN 1 ELSE 0
        END AS is_trip_start

    FROM {{ ref('stg_einride_telemetry') }}

),

numbered AS (

    SELECT
        *,
        SUM(is_trip_start) OVER (
            PARTITION BY vehicle_id ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS trip_seq,

        MAX(event_time) OVER (PARTITION BY vehicle_id) AS vehicle_last_seen_at

    FROM marked
    WHERE route_id IS NOT NULL

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['vehicle_id', 'trip_seq']) }} AS trip_id,
    vehicle_id,
    trip_seq,
    MAX(route_id)                                       AS route_id,
    MAX(shipment_id)                                    AS shipment_id,
    MAX(customer_id)                                    AS customer_id,
    MAX(driver_id)                                      AS driver_id,

    MIN(event_time)                                     AS trip_started_at,
    MAX(event_time)                                     AS trip_ended_at,
    (UNIX_TIMESTAMP(MAX(event_time))
       - UNIX_TIMESTAMP(MIN(event_time))) / 60.0        AS trip_duration_minutes,

    -- The meters are lifetime odometers, so a trip is the difference between
    -- the first and last reading. Never a sum.
    MAX_BY(distance_meter_km, event_time)
      - MIN_BY(distance_meter_km, event_time)           AS trip_distance_km,
    MAX_BY(energy_meter_kwh, event_time)
      - MIN_BY(energy_meter_kwh, event_time)            AS trip_energy_kwh,

    MIN_BY(state_of_charge_pct, event_time)             AS soc_start_pct,
    MAX_BY(state_of_charge_pct, event_time)             AS soc_end_pct,

    AVG(speed_kmh)                                      AS avg_speed_kmh,
    MAX(speed_kmh)                                      AS max_speed_kmh,
    COUNT(*)                                            AS ping_count,

    MAX(CASE WHEN trigger_type = 'IGNITION_OFF' THEN TRUE ELSE FALSE END)
                                                        AS has_ignition_off

FROM numbered

GROUP BY vehicle_id, trip_seq

-- Closed only. Either the vehicle logged off, or it has reported since on
-- something else. The 43 trips still in flight fall out here and get picked up
-- on a later run.
HAVING MAX(CASE WHEN trigger_type = 'IGNITION_OFF' THEN 1 ELSE 0 END) = 1
    OR MAX(event_time) < MAX(vehicle_last_seen_at)
