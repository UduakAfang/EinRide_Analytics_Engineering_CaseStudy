-- One row per alert episode. Not per message -- a latched fault re-asserts
-- every 15 minutes and counting those would report a fleet five times sicker
-- than it is.

SELECT
    a.alert_id,
    a.vehicle_id,
    a.alert_code,
    a.alert_kind,
    a.severity,
    a.alert_description,

    v.vehicle_type,
    v.oem,
    v.home_depot_id,
    v.home_depot_region,

    a.alert_started_at,
    a.alert_last_seen_at,
    DATE(a.alert_started_at)                        AS alert_date,
    a.alert_duration_minutes,

    a.first_event_id,
    a.reported_on_rows,
    a.had_tell_tale

FROM {{ ref('int_einride_alerts') }} a

LEFT JOIN {{ ref('dim_vehicle') }} v
       ON a.vehicle_id = v.vehicle_id
