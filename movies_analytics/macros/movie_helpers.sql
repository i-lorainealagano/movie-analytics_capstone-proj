{% macro prev_year_metric(column, partition_by='release_year', entity_col=null) %}
    {% if entity_col %}
        LAG({{ column }}) OVER (PARTITION BY {{ entity_col }} ORDER BY {{ partition_by }})
    {% else %}
        LAG({{ column }}) OVER (ORDER BY {{ partition_by }})
    {% endif %}
{% endmacro %}

{% macro rating_change(current_col, prev_col) %}
    {{ current_col }} - {{ prev_col }}
{% endmacro %}

{% macro diversity_index(genre_div_col, lang_div_col, avg_rating_col) %}
    ({{ genre_div_col }} + {{ lang_div_col }}) * {{ avg_rating_col }}
{% endmacro %}

{% macro diversity_score_raw(genre_div_col, lang_div_col) %}
    ({{ genre_div_col }} + {{ lang_div_col }})
{% endmacro %}

{% macro profitability_flag(revenue_col, budget_col) %}
    CASE 
        WHEN {{ revenue_col }} >= {{ budget_col }} THEN 'Profitable'
        ELSE 'Not Profitable'
    END
{% endmacro %}

{% macro language_type(lang_div_col) %}
    CASE 
        WHEN {{ lang_div_col }} > 1 THEN 'Multilingual'
        ELSE 'Single Language'
    END
{% endmacro %}

{% macro rating_flag(avg_rating_col) %}
    CASE
        WHEN {{ avg_rating_col }} < 0 OR {{ avg_rating_col }} > 10 THEN 'Check'
        ELSE 'OK'
    END
{% endmacro %}