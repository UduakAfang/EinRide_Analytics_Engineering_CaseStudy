-- One row per driver per version of their record. Pods have no driver, so this
-- is the human half of the fleet only.
--
-- Type 2, on two columns. license_class is a compliance fact -- what the driver
-- was allowed to operate on the day -- and home_depot_id is how their work gets
-- attributed to a site. Neither is stamped on a trip, so without history both
-- get rewritten backwards the moment someone is promoted or transferred.
--
-- driver_id repeats here. Facts join on driver_key.
--
-- Name and hire date come along and are overwritten with no version. A
-- misspelled name is a correction, not a new version of the person.

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.driver_id', 's.dbt_valid_from']) }}
                                                    AS driver_key,
    s.driver_id,

    s.driver_name,
    s.license_class,
    CAST(s.hire_date AS DATE)                       AS hire_date,

    -- Tenure is measured from today on purpose. It is the answer to "how
    -- experienced is this driver now", not a property of any past version.
    DATEDIFF(CURRENT_DATE(), CAST(s.hire_date AS DATE)) / 365.25
                                                    AS tenure_years,

    s.home_depot_id,
    dep.depot_name                                  AS home_depot_name,
    dep.region                                      AS home_depot_region,

    s.dbt_valid_from                                AS valid_from,
    s.dbt_valid_to                                  AS valid_to,
    s.dbt_valid_to IS NULL                          AS is_current

FROM {{ ref('snap_drivers') }} s

LEFT JOIN {{ ref('stg_einride_depots') }} dep
       ON s.home_depot_id = dep.depot_id
