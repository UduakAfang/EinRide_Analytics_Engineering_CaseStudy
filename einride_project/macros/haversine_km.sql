{#
  Straight-line distance between two points, in kilometres.

  A macro and not a model. A model is a table someone queries; this is a
  formula, and nobody wants to SELECT from a list of distances. Written once
  here, it cannot drift between the places that use it.

  Haversine rather than a flat subtraction of degrees, because a degree of
  longitude is 111 km at the equator and 46 km in Luleå. Sweden is long enough
  north to south that the flat version is wrong by tens of kilometres.

      2R * ASIN(SQRT(a)),  R = 6371 km,  so 12742 = 2R

  Takes column expressions, not values. Pass whatever the query calls them.
#}

{% macro haversine_km(lat_a, lon_a, lat_b, lon_b) %}
    12742 * ASIN(
        SQRT(
            POWER(SIN(RADIANS(({{ lat_a }} - {{ lat_b }}) / 2.0)), 2)
            + COS(RADIANS({{ lat_a }}))
              * COS(RADIANS({{ lat_b }}))
              * POWER(SIN(RADIANS(({{ lon_a }} - {{ lon_b }}) / 2.0)), 2)
        )
    )
{% endmacro %}
