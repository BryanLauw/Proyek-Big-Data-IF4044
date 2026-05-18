{% macro calculate_revenue(extendedprice, discount) %}
    ({{ extendedprice }} * (1 - {{ discount }}))
{% endmacro %}
