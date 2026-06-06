{% macro category_cost_rate(category_expression) -%}
case
  when {{ category_expression }} = 'FMCG' then 0.72
  when {{ category_expression }} = 'ELHA' then 0.82
  when {{ category_expression }} = 'Fashion' then 0.55
  when {{ category_expression }} = 'Home & Living' then 0.62
  else 0.65
end
{%- endmacro %}

{% macro json_string(json_expression, path) -%}
json_extract_string(to_json({{ json_expression }}), '{{ path }}')
{%- endmacro %}

{% macro json_double(json_expression, path) -%}
try_cast({{ json_string(json_expression, path) }} as double)
{%- endmacro %}

{% macro json_int(json_expression, path) -%}
try_cast({{ json_string(json_expression, path) }} as integer)
{%- endmacro %}
