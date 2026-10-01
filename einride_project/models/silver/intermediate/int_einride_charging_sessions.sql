-- One row per charging session.
--
-- No sessionising here. The generator stamps charge_session_id, so a group by
-- is enough. Trips have to be worked out; charging does not.
--
-- Most sessions are still open. Charging takes hours and the telemetry window
-- is six, so most have no CHARGING_STOPPED. I keep them and flag it.

WITH sessions AS (

    SELECT
        t.charge_session_id,
        t.vehicle_id,

        MIN(t.event_time)                                AS charge_started_at,

        -- Last ping, not the unplug. Only is_complete says which.
        MAX(t.event_time)                                AS charge_last_seen_at,
        (UNIX_TIMESTAMP(MAX(t.event_time))
           - UNIX_TIMESTAMP(MIN(t.event_time))) / 60.0   AS charge_duration_minutes,

        MIN_BY(t.state_of_charge_pct, t.event_time)      AS soc_start_pct,
        MAX_BY(t.state_of_charge_pct, t.event_time)      AS soc_end_pct,

        -- Position at the first ping. Averaging the session would drift if the
        -- vehicle moved at all.
        MIN_BY(t.latitude, t.event_time)                 AS start_latitude,
        MIN_BY(t.longitude, t.event_time)                AS start_longitude,

        MAX(t.charge_power_kw)                           AS peak_charge_power_kw,
        COUNT(*)                                         AS ping_count,

        -- True only if the truck actually unplugged.
        MAX(CASE WHEN t.trigger_type = 'CHARGING_STOPPED' THEN TRUE ELSE FALSE END)
                                                         AS is_complete,

        -- Needed to turn a percentage into kWh.
        MAX(v.battery_capacity_kwh)                      AS battery_capacity_kwh,
        AVG(t.state_of_health_pct)                       AS avg_state_of_health_pct

    FROM {{ ref('stg_einride_telemetry') }} t

    LEFT JOIN {{ ref('int_einride_vehicles') }} v
           ON t.vehicle_id = v.vehicle_id

    WHERE t.charge_session_id IS NOT NULL

    GROUP BY t.charge_session_id, t.vehicle_id

),

depots AS (

    -- Charging only ever happens at a depot, and the coordinates match the
    -- depot's exactly -- all 112 sessions, no exceptions. So this is a plain
    -- join on position, not a nearest-neighbour search.
    --
    -- LEFT, not INNER. If a future run ever charges on the road, depot_id comes
    -- back null and I see it, instead of the row quietly disappearing.
    SELECT
        depot_id,
        ROUND(latitude, 4)                               AS latitude,
        ROUND(longitude, 4)                              AS longitude

    FROM {{ ref('stg_einride_depots') }}

)

SELECT
    s.charge_session_id,
    s.vehicle_id,

    s.charge_started_at,
    s.charge_last_seen_at,
    s.charge_duration_minutes,
    s.is_complete,

    s.soc_start_pct,
    s.soc_end_pct,
    s.soc_end_pct - s.soc_start_pct                      AS soc_gained_pct,

    -- From the battery, not the meter. energy_meter_kwh counts energy the truck
    -- has burned, and a parked truck burns nothing, so differencing it here
    -- returned zero on every session.
    ROUND((s.soc_end_pct - s.soc_start_pct) / 100.0
          * s.battery_capacity_kwh
          * s.avg_state_of_health_pct / 100.0, 2)        AS energy_added_kwh,

    s.peak_charge_power_kw,
    s.ping_count,

    ROUND(s.start_latitude, 4)                           AS latitude,
    ROUND(s.start_longitude, 4)                          AS longitude,

    d.depot_id,

    -- No depot means the truck charged somewhere else. Does not happen today,
    -- and I want to know the day it does.
    CASE WHEN d.depot_id IS NOT NULL THEN 'DEPOT'
         ELSE 'EN_ROUTE' END                             AS charge_location_type

FROM sessions s

LEFT JOIN depots d
       ON ROUND(s.start_latitude, 4) = d.latitude
      AND ROUND(s.start_longitude, 4) = d.longitude
