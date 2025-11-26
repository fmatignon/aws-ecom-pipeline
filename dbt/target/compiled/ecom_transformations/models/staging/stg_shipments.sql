



with source as (
    select *
    from "AwsDataCatalog"."ecom_bronze"."shipments"
    
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by tracking_number
                order by _ingestion_ts desc
            ) as _dedup_row_num
        from source
    ) ranked
    where _dedup_row_num = 1
),

cleaned as (
    select
        -- Primary key
        cast(tracking_number as bigint) as tracking_number,
        tracking_code,

        -- Foreign key
        order_id,

        -- Shipment details
        shipping_carrier,
        origin_country,
        destination_country,
        destination_postal_code,

        -- Status (normalized to lowercase with accepted values mapping)
        case
            when lower(status) in ('pending', 'in_transit', 'delivered', 'returned', 'cancelled', 'exception')
            then lower(status)
            else lower(status)
        end as status,

        -- Shipping cost
        cast(shipping_cost as decimal(10,2)) as shipping_cost,

        -- Parse shipment_date from varchar to timestamp
        coalesce(
            try_cast(from_iso8601_timestamp("shipment-date") as timestamp),
            try(date_parse("shipment-date", '%Y-%m-%d %H:%i:%s'))
        ) as shipment_date,
        case 
            when coalesce(
                try_cast(from_iso8601_timestamp("shipment-date") as timestamp),
                try(date_parse("shipment-date", '%Y-%m-%d %H:%i:%s'))
            ) is null 
            and "shipment-date" is not null 
            then true 
            else false 
        end as shipment_date_parse_error,

        -- Parse estimated_delivery_date from varchar to timestamp
        coalesce(
            try_cast(from_iso8601_timestamp("estimated-delivery-date") as timestamp),
            try(date_parse("estimated-delivery-date", '%Y-%m-%d %H:%i:%s'))
        ) as estimated_delivery_date,
        case 
            when coalesce(
                try_cast(from_iso8601_timestamp("estimated-delivery-date") as timestamp),
                try(date_parse("estimated-delivery-date", '%Y-%m-%d %H:%i:%s'))
            ) is null 
            and "estimated-delivery-date" is not null 
            then true 
            else false 
        end as estimated_delivery_date_parse_error,

        -- Parse actual_delivery_date from varchar to timestamp
        coalesce(
            try_cast(from_iso8601_timestamp("actual-delivery-date") as timestamp),
            try(date_parse("actual-delivery-date", '%Y-%m-%d %H:%i:%s'))
        ) as actual_delivery_date,
        case 
            when coalesce(
                try_cast(from_iso8601_timestamp("actual-delivery-date") as timestamp),
                try(date_parse("actual-delivery-date", '%Y-%m-%d %H:%i:%s'))
            ) is null 
            and "actual-delivery-date" is not null 
            then true 
            else false 
        end as actual_delivery_date_parse_error,

        -- Parse created_at from varchar to timestamp
        coalesce(
            try_cast(from_iso8601_timestamp("created-at") as timestamp),
            try(date_parse("created-at", '%Y-%m-%d %H:%i:%s'))
        ) as created_at,
        case 
            when coalesce(
                try_cast(from_iso8601_timestamp("created-at") as timestamp),
                try(date_parse("created-at", '%Y-%m-%d %H:%i:%s'))
            ) is null 
            and "created-at" is not null 
            then true 
            else false 
        end as created_at_parse_error,

        -- Parse updated_at from varchar to timestamp
        coalesce(
            try_cast(from_iso8601_timestamp("updated-at") as timestamp),
            try(date_parse("updated-at", '%Y-%m-%d %H:%i:%s'))
        ) as updated_at,
        case 
            when coalesce(
                try_cast(from_iso8601_timestamp("updated-at") as timestamp),
                try(date_parse("updated-at", '%Y-%m-%d %H:%i:%s'))
            ) is null 
            and "updated-at" is not null 
            then true 
            else false 
        end as updated_at_parse_error,

        -- Audit columns
        _ingestion_ts,
        _source_system,
        _record_hash,
        ingestion_date

    from deduplicated
)

select * from cleaned