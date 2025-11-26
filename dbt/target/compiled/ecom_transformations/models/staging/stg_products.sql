



with source as (
    select *
    from "AwsDataCatalog"."ecom_bronze"."products"
    
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