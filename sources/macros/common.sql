{% macro fqn(schema, object) %}{{ db }}.{{ schema }}.{{ object }}{% endmacro %}
