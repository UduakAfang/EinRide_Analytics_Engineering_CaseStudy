-- One row per driver. Pods have none, so this covers the human half of the
-- fleet only.

SELECT
    d.driver_id,
    d.driver_name,
    d.license_class,
    d.hire_date,
    DATEDIFF(CURRENT_DATE(), d.hire_date) / 365.25  AS tenure_years,

    d.home_depot_id,
    dep.depot_name                                  AS home_depot_name,
    dep.region                                      AS home_depot_region

FROM {{ ref('stg_einride_drivers') }} d

LEFT JOIN {{ ref('stg_einride_depots') }} dep
       ON d.home_depot_id = dep.depot_id
