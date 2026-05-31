{% macro reset_gold_schema() %}
  {% if flags.WHICH in ['build', 'run'] %}
    {% set gold_schema = api.Relation.create(database=target.database, schema='gold') %}
    {% do adapter.drop_schema(gold_schema) %}
    {% do run_query('create schema if not exists gold') %}
  {% endif %}
{% endmacro %}
