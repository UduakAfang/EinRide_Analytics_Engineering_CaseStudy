-- One row per alert episode.
--
-- Two kinds of alert arrive in this feed and they are not the same shape. A
-- state comes on and stays on, re-asserting every 15 minutes until it clears.
-- An event happens and is already over -- braking hard takes two seconds.
--
-- So the gap that starts a new episode depends on the kind, and the kind comes
-- from the seed rather than from anything hardcoded here. A state gets the
-- full threshold. An event gets zero, which merges only rows sharing a
-- timestamp: the TELL_TALE and its TIMER twin, and nothing else.

WITH exploded AS (

    SELECT
        event_id,
        event_time,
        vehicle_id,
        vehicle_type,
        oem,
        trigger_type,
        EXPLODE(FROM_JSON(alerts_json, 'array<string>')) AS alert_code

    FROM {{ ref('stg_einride_telemetry') }}

    WHERE alerts_json IS NOT NULL
      AND alerts_json <> '[]'

),

classified AS (

    SELECT
        e.*,
        c.alert_kind,
        c.severity,
        c.description                                   AS alert_description,

        -- An unknown code is treated as an event. Safer to report it twice than
        -- to merge two real faults, and the relationships test names it anyway.
        CASE WHEN c.alert_kind = 'state'
             THEN {{ var('alert_gap_minutes') }} ELSE 0 END AS gap_threshold_minutes

    FROM exploded e
    LEFT JOIN {{ ref('einride_alert_codes') }} c
           ON e.alert_code = c.alert_code

),

gapped AS (

    SELECT
        *,

        CASE
            WHEN TIMESTAMPDIFF(MINUTE,
                     LAG(event_time) OVER (
                         PARTITION BY vehicle_id, alert_code ORDER BY event_time),
                     event_time) > gap_threshold_minutes
              OR LAG(event_time) OVER (
                     PARTITION BY vehicle_id, alert_code ORDER BY event_time) IS NULL
            THEN 1 ELSE 0
        END AS is_new_episode

    FROM classified

),

numbered AS (

    SELECT
        *,
        SUM(is_new_episode) OVER (
            PARTITION BY vehicle_id, alert_code ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_seq

    FROM gapped

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['vehicle_id', 'alert_code', 'episode_seq']) }} AS alert_id,
    vehicle_id,
    alert_code,
    MAX(alert_kind)                                     AS alert_kind,
    MAX(severity)                                       AS severity,
    MAX(alert_description)                              AS alert_description,
    MAX(vehicle_type)                                   AS vehicle_type,
    MAX(oem)                                            AS oem,

    MIN(event_time)                                     AS alert_started_at,
    MAX(event_time)                                     AS alert_last_seen_at,
    TIMESTAMPDIFF(MINUTE, MIN(event_time), MAX(event_time))
                                                        AS alert_duration_minutes,

    MIN_BY(event_id, event_time)                        AS first_event_id,
    COUNT(*)                                            AS reported_on_rows,

    MAX(CASE WHEN trigger_type = 'TELL_TALE' THEN TRUE ELSE FALSE END)
                                                        AS had_tell_tale

FROM numbered


GROUP BY vehicle_id, alert_code, episode_seq
