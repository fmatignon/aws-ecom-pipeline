"""
Deduplication macro that removes duplicate records using _record_hash when available,
or falls back to row_number() partitioned by primary key ordered by _ingestion_ts descending.
"""

{% macro dedupe_using_record_hash(source_table, primary_key_columns, record_hash_column='_record_hash', ingestion_ts_column='_ingestion_ts') %}
{#
    Deduplicates a source table using _record_hash if reliable, otherwise uses
    row_number() partitioned by primary key(s) and ordered by _ingestion_ts desc.

    Args:
        source_table: The source table reference or CTE name to deduplicate.
        primary_key_columns: List of column names forming the primary key (e.g., ['customer_id'] or ['order_id', 'line_id']).
        record_hash_column: Name of the record hash column (default: '_record_hash').
        ingestion_ts_column: Name of the ingestion timestamp column (default: '_ingestion_ts').

    Returns:
        SQL fragment that produces deduplicated rows (use as a CTE or subquery).
#}

{% set pk_list = primary_key_columns | join(', ') %}

(
    select *
    from (
        select
            *,
            row_number() over (
                partition by {{ pk_list }}
                order by {{ ingestion_ts_column }} desc
            ) as _dedup_row_num
        from {{ source_table }}
    ) ranked
    where _dedup_row_num = 1
)

{% endmacro %}

