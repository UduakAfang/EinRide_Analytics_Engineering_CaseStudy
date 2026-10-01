-- What a kWh costs and how dirty it is, by price zone, hour and day type.
-- 4 zones x 24 hours x 2 day types = 192 rows.
--
-- Price and carbon live in two tables upstream. They share zone and hour, so
-- they sit together here and every model that needs either joins once.
--
-- Carbon has no weekday/weekend split, so the same value lands on both.

SELECT
    CONCAT(t.price_zone, '-', LPAD(CAST(t.hour_of_day AS STRING), 2, '0'), '-', t.day_type)
                                                    AS energy_price_key,
    t.price_zone,
    t.hour_of_day,
    t.day_type,
    t.price_sek_per_kwh,
    g.carbon_intensity_gco2_per_kwh

FROM {{ ref('stg_einride_tariffs') }} t

LEFT JOIN {{ ref('stg_einride_grid_carbon_intensity') }} g
       ON t.price_zone = g.price_zone
      AND t.hour_of_day = g.hour_of_day
