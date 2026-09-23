-- One row per charging session.
--
-- No sessionising here. The generator stamps charge_session_id, so a group by
-- is enough. Trips have to be worked out; charging does not.
--
-- Most sessions are open. Charging takes hours and the telemetry window is six,
-- so only 14 of 84 have a CHARGING_STOPPED. I keep them all and flag it.

SELECT
    charge_session_id,
    vehicle_id,

    MIN(event_time)                                     AS charge_started_at,
    MAX(event_time)                                     AS charge_ended_at,
    (UNIX_TIMESTAMP(MAX(event_time))
       - UNIX_TIMESTAMP(MIN(event_time))) / 60.0        AS charge_duration_minutes,

    MIN_BY(state_of_charge_pct, event_time)             AS soc_start_pct,
    MAX_BY(state_of_charge_pct, event_time)             AS soc_end_pct,

    MAX_BY(energy_meter_kwh, event_time)
      - MIN_BY(energy_meter_kwh, event_time)            AS energy_added_kwh,

    MAX(charge_power_kw)                                AS peak_charge_power_kw,
    ROUND(AVG(latitude), 4)                             AS latitude,
    ROUND(AVG(longitude), 4)                            AS longitude,
    COUNT(*)                                            AS ping_count,

    MAX(CASE WHEN trigger_type = 'CHARGING_STOPPED' THEN TRUE ELSE FALSE END)
                                                        AS is_complete

FROM {{ ref('stg_einride_telemetry') }}

WHERE charge_session_id IS NOT NULL

GROUP BY charge_session_id, vehicle_id
