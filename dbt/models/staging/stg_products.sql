{#
"""
Staging model for products that cleans, types, and deduplicates
the bronze products table. Normalizes column names for downstream use.
"""
#}

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='insert_overwrite',
        partitioned_by=['ingestion_date'],
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select *
    from {{ source('ecom_bronze', 'products') }}
    {% if is_incremental() %}
    where _ingestion_ts > (select coalesce(max(_ingestion_ts), timestamp '1970-01-01') from {{ this }})
    {% endif %}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by product_id
                order by _ingestion_ts desc
            ) as _dedup_row_num
        from source
    ) ranked
    where _dedup_row_num = 1
),

cleaned as (
    select
        -- Primary key
        product_id,

        -- Product details
        trim(product_name) as product_name,
        lower(trim(category)) as category,
        lower(trim(sub_category)) as sub_category,
        trim(brand) as brand,

        -- Pricing (keep decimal types)
        price,
        cost,

        -- Metadata
        created_at,
        lower(trim(color)) as color,

        -- Audit columns
        _ingestion_ts,
        _source_system,
        _record_hash,

        -- Partition column must be last
        ingestion_date

    from deduplicated
)

select * from cleaned

