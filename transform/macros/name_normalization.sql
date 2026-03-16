{% macro normalize_player_name(column_name) %}
    trim(lower(strip_accents({{ column_name }})))
{% endmacro %}

{% macro normalize_team_name(column_name) %}
    trim(lower(strip_accents({{ column_name }})))
{% endmacro %}
