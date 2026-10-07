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

    -- The parked rows stay. IGNITION_OFF arrives after the generator has
    -- already cleared the route, so filtering on route_id here deleted every
    -- one of them and has_ignition_off could never be true.
    --
    -- A parked row carries the trip_seq of the trip it follows, because the
    -- running total only moves on a start. So the tail belongs to its trip,
    -- which is exactly where the IGNITION_OFF is.

)

-- NOTE: MAX_BY/MIN_BY take the value at the latest/earliest event of the whole
-- group, and the group runs on past the last route reading (the vehicle goes
-- idle, route_id is null). The old CASE inside MAX_BY therefore returned NULL
-- for most trips. FILTER restricts the pick to route readings only.
SELECT
    {{ dbt_utils.generate_surrogate_key(['vehicle_id', 'trip_seq']) }} AS trip_id,
    vehicle_id,
    trip_seq,
    MAX(route_id)                                       AS route_id,
    MAX(shipment_id)                                    AS shipment_id,
    MAX(customer_id)                                    AS customer_id,
    MAX(driver_id)                                      AS driver_id,

    -- Driving rows only. The parked tail is in this group to carry the
    -- IGNITION_OFF, and counting it would add the time spent standing still
    -- at the depot to the trip.
    MIN(CASE WHEN route_id IS NOT NULL THEN event_time END)
                                                        AS trip_started_at,
    MAX(CASE WHEN route_id IS NOT NULL THEN event_time END)
                                                        AS trip_ended_at,
    (UNIX_TIMESTAMP(MAX(CASE WHEN route_id IS NOT NULL THEN event_time END))
       - UNIX_TIMESTAMP(MIN(CASE WHEN route_id IS NOT NULL THEN event_time END)))
       / 60.0                                           AS trip_duration_minutes,

    -- The meters are lifetime odometers, so a trip is the difference between
    -- the first and last reading. Never a sum.
    MAX_BY(distance_meter_km, event_time) FILTER (WHERE route_id IS NOT NULL)
      - MIN_BY(distance_meter_km, event_time) FILTER (WHERE route_id IS NOT NULL)
                                                        AS trip_distance_km,
    MAX_BY(energy_meter_kwh, event_time) FILTER (WHERE route_id IS NOT NULL)
      - MIN_BY(energy_meter_kwh, event_time) FILTER (WHERE route_id IS NOT NULL)
                                                        AS trip_energy_kwh,

    MIN_BY(state_of_charge_pct, event_time) FILTER (WHERE route_id IS NOT NULL)
                                                        AS soc_start_pct,
    MAX_BY(state_of_charge_pct, event_time) FILTER (WHERE route_id IS NOT NULL)
                                                        AS soc_end_pct,

    AVG(CASE WHEN route_id IS NOT NULL THEN speed_kmh END)
                                                        AS avg_speed_kmh,
    MAX(CASE WHEN route_id IS NOT NULL THEN speed_kmh END)
                                                        AS max_speed_kmh,
    SUM(CASE WHEN route_id IS NOT NULL THEN 1 ELSE 0 END)
                                                        AS ping_count,

    MAX(CASE WHEN trigger_type = 'IGNITION_OFF' THEN TRUE ELSE FALSE END)
                                                        AS has_ignition_off

FROM numbered

GROUP BY vehicle_id, trip_seq

-- Must contain driving rows. trip_seq 0 is whatever the vehicle did before
-- its first trip, and that is not a trip.
HAVING MAX(CASE WHEN route_id IS NOT NULL THEN 1 ELSE 0 END) = 1

   -- Closed either way: the truck said IGNITION_OFF, or it has reported since
   -- on something else, so this trip must be over even if we never saw it end.
   AND (MAX(CASE WHEN trigger_type = 'IGNITION_OFF' THEN 1 ELSE 0 END) = 1
        OR MAX(CASE WHEN route_id IS NOT NULL THEN event_time END)
             < MAX(vehicle_last_seen_at))
