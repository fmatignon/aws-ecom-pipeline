



with source as (
    select *
    from "AwsDataCatalog"."ecom_bronze"."payments"
    
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by "payment-id"
                order by _ingestion_ts desc
            ) as _dedup_row_num
        from source
    ) ranked
    where _dedup_row_num = 1
),

cleaned as (
    select
        -- Primary key (map hyphenated name to snake_case)
        cast("payment-id" as bigint) as payment_id,

        -- Foreign keys
        cast("order-id" as bigint) as order_id,
        cast("customer-id" as bigint) as customer_id,

        -- Payment details
        lower("payment-method") as payment_method,
        lower("card-brand") as card_brand,
        "card-last-4" as card_last_4,

        -- Amount (cast to decimal for precision)
        cast(amount as decimal(12,2)) as amount,

        -- Status (normalized to lowercase)
        lower("payment-status") as payment_status,

        -- Parse payment_date from varchar to timestamp
        coalesce(
            try_cast(from_iso8601_timestamp("payment-date") as timestamp),
            try(date_parse("payment-date", '%Y-%m-%d %H:%i:%s'))
        ) as payment_date,
        case 
            when coalesce(
                try_cast(from_iso8601_timestamp("payment-date") as timestamp),
                try(date_parse("payment-date", '%Y-%m-%d %H:%i:%s'))
            ) is null 
            and "payment-date" is not null 
            then true 
            else false 
        end as payment_date_parse_error,

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