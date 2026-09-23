SELECT
    price_zone,
    hour_of_day,
    carbon_intensity_gco2_per_kwh

FROM {{source ('bronze', 'grid_carbon_intensity')}}
