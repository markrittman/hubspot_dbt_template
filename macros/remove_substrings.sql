-- This macro removes all the strings from the target column.
-- strings_to_be_stripped should be passed as an array of strings
{%- macro remove_substrings(column, strings_to_be_removed) -%}

    {%- for string in strings_to_be_removed -%}
        REPLACE(
    {%- endfor -%}

        {{ column }},

    {%- for string in strings_to_be_removed -%}
        '{{ string }}', '')
        {% if not loop.last -%} , {%- endif -%}
    {%- endfor -%}

{%- endmacro -%}
