-- One row per day, from the first day of the rollup to today.
--
-- Power BI needs this for anything over time. Without a date table it builds
-- hidden ones per column and two charts stop agreeing on what a week is.
--
-- Starts at the rollup's first day, not a hardcoded date, so a regenerated
-- history moves it with it.

WITH bounds AS (

    SELECT MIN(rollup_date) AS first_day
    FROM {{ ref('stg_einride_daily_rollup') }}

),

days AS (

    -- SEQUENCE makes the list of days, EXPLODE turns the list into rows. It
    -- needs first_day to exist already, which is why bounds comes first.
    SELECT EXPLODE(SEQUENCE(first_day, CURRENT_DATE(), INTERVAL 1 DAY)) AS date_day
    FROM bounds

)

SELECT
    date_day,
    YEAR(date_day)                                  AS year,
    QUARTER(date_day)                               AS quarter,
    CONCAT('Q', QUARTER(date_day), ' ', YEAR(date_day))
                                                    AS quarter_label,
    MONTH(date_day)                                 AS month_num,
    DATE_FORMAT(date_day, 'MMM')                    AS month_name,
    CAST(DATE_TRUNC('MONTH', date_day) AS DATE)     AS month_start,
    WEEKOFYEAR(date_day)                            AS iso_week,

    -- Monday is 1. Spark's own DAYOFWEEK starts on Sunday, which is not how
    -- anyone in Sweden reads a week.
    WEEKDAY(date_day) + 1                           AS day_of_week,
    DATE_FORMAT(date_day, 'E')                      AS day_name,
    {{ day_type('date_day') }}                      AS day_type,
    DAYOFWEEK(date_day) IN (1, 7)                   AS is_weekend

FROM days
