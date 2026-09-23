SELECT
    dispatch_id,
    shipment_id,
    customer_id,
    vehicle_id,
    vehicle_type,
    route_id,
    origin_depot_id AS shipment_origin_depot_id,
    dest_depot_id AS shipment_dest_depot_id,
    planned_distance_km,
    cargo_weight_kg,
    dispatched_at,
    promised_arrival_at,
    actual_arrival_at,
    delivered_on_time,
    is_deadhead

FROM {{source ('bronze', 'shipments')}}
