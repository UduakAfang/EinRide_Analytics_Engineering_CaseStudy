SELECT 
    driver_id,
    driver_name,
    home_depot_id,
    license_class,
    (CAST( hire_date AS DATE)) AS hire_date

 FROM {{source ('bronze', 'drivers')}}