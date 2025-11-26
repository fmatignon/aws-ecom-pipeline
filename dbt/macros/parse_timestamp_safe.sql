"""
Safe timestamp parsing macro that attempts to parse varchar columns to timestamps
using Athena's from_iso8601_timestamp with TRY for error handling, and produces
a parse_error boolean flag when parsing fails.
"""

{% macro parse_timestamp_safe(source_column, output_column) %}
{#
    Safely parses a varchar column to timestamp, returning NULL on parse failure
    and setting a boolean error flag.

    Args:
        source_column: The varchar column name containing the date/timestamp string.
        output_column: The desired output column name for the parsed timestamp.

    Returns:
        Two columns: the parsed timestamp and a boolean parse_error flag.

    Usage in SELECT:
        {{ parse_timestamp_safe('payment-date', 'payment_date') }}
    
    Produces:
        try(from_iso8601_timestamp("payment-date")) as payment_date,
        case when try(from_iso8601_timestamp("payment-date")) is null 
             and "payment-date" is not null then true else false end as payment_date_parse_error
#}

try(from_iso8601_timestamp({{ source_column }})) as {{ output_column }},
case 
    when try(from_iso8601_timestamp({{ source_column }})) is null 
         and {{ source_column }} is not null 
    then true 
    else false 
end as {{ output_column }}_parse_error

{% endmacro %}


{% macro parse_timestamp_safe_athena(source_column, output_column) %}
{#
    Alternative parsing for standard timestamp formats (YYYY-MM-DD HH:MM:SS)
    when ISO8601 parsing fails. Uses date_parse for Athena compatibility.

    Args:
        source_column: The varchar column name containing the date/timestamp string.
        output_column: The desired output column name for the parsed timestamp.

    Returns:
        Two columns: the parsed timestamp and a boolean parse_error flag.
#}

coalesce(
    try(from_iso8601_timestamp({{ source_column }})),
    try(date_parse({{ source_column }}, '%Y-%m-%d %H:%i:%s'))
) as {{ output_column }},
case 
    when coalesce(
        try(from_iso8601_timestamp({{ source_column }})),
        try(date_parse({{ source_column }}, '%Y-%m-%d %H:%i:%s'))
    ) is null 
    and {{ source_column }} is not null 
    then true 
    else false 
end as {{ output_column }}_parse_error

{% endmacro %}

