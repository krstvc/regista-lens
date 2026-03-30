{% test xg_per_shot_range(model, column_name, shots_column) %}
{#
    Validates that xG per shot does not exceed 1.0.
    Any single shot can have at most 1.0 xG, so aggregate xG / shots > 1.0
    indicates data corruption.
#}

select *
from {{ model }}
where {{ shots_column }} > 0
  and {{ column_name }} / {{ shots_column }} > 1.0

{% endtest %}
