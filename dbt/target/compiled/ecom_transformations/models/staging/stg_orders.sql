



with source as (
    select *
    from "AwsDataCatalog"."ecom_bronze"."orders"
    
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by order_id
                order by _ingestion_ts desc
            ) as _dedup_row_num
        from source
    ) ranked
    where _dedup_row_num = 1
),

cleaned as (
    select
        -- Primary key
        order_id,

        -- Foreign key
        customer_id,

        -- Timestamps
        order_date,
        payment_date,
        shipment_date,
        delivered_date,
        created_at,
        updated_at,

        -- Status fields (normalized to lowercase)
        lower(order_status) as order_status,
        lower(payment_status) as payment_status,

        -- Monetary columns (keep decimal types)
        subtotal,
        discount_amount,
        tax_amount,
        shipping_cost,
        total_amount,

        -- Payment and shipping details
        lower(payment_method) as payment_method,
        cast(payment_id as bigint) as payment_id,
        shipping_carrier,
        cast(tracking_number as varchar) as tracking_number,

        -- Segment at time of order
        customer_segment_at_order,

        -- QA Flags: is_paid (payment completed successfully)
        case
            when lower(payment_status) in ('completed', 'success')
            then true
            else false
        end as is_paid,

        -- QA Flags: is_delivered (order delivered or has delivery date)
        case
            when lower(order_status) = 'delivered'
                 or delivered_date is not null
            then true
            else false
        end as is_delivered,

        -- QA Flags: is_refunded (order or payment was refunded)
        case
            when lower(order_status) = 'refunded'
                 or lower(payment_status) = 'refunded'
            then true
            else false
        end as is_refunded,

        -- QA Flags: is_cancelled (order was cancelled)
        case
            when lower(order_status) = 'cancelled'
            then true
            else false
        end as is_cancelled,

        -- Audit columns
        _ingestion_ts,
        _source_system,
        _record_hash,

        -- Partition column must be last
        ingestion_date

    from deduplicated
)

select * from cleaned