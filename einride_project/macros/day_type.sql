{% macro day_type(ts) %}
    -- Swedish tariffs are priced weekday or weekend. Spark counts Sunday as 1
    -- and Saturday as 7. Written once here so the tariff join, the date
    -- dimension and anything else can't disagree about what a weekend is.
    CASE WHEN DAYOFWEEK({{ ts }}) IN (1, 7) THEN 'weekend' ELSE 'weekday' END
{% endmacro %}
