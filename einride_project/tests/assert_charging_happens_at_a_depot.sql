-- int_einride_charging_sessions joins depots on an exact coordinate match.
-- That only works because the generator parks the vehicle on the depot's own
-- coordinates before it plugs in. This is the test that says so out loud.
--
-- Returns the sessions that are more than 1 km from every depot. Any row here
-- means the assumption broke and the exact join is now silently returning null
-- instead of a depot.

SELECT
    c.charge_session_id,
    c.vehicle_id,
    c.latitude,
    c.longitude,
    MIN({{ haversine_km('c.latitude', 'c.longitude', 'd.latitude', 'd.longitude') }})
                                                     AS km_to_closest_depot

FROM {{ ref('int_einride_charging_sessions') }} c
CROSS JOIN {{ ref('stg_einride_depots') }} d

GROUP BY c.charge_session_id, c.vehicle_id, c.latitude, c.longitude
HAVING MIN({{ haversine_km('c.latitude', 'c.longitude', 'd.latitude', 'd.longitude') }}) > 1
