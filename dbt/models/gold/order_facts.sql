{#
"""
Gold model for order facts at the grain of order_id. Joins orders with the latest
payment and shipment records to provide a comprehensive view of each order's
lifecycle including payment, shipping, and delivery status.
"""
#}

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partitioned_by=['order_date_partition'],
        file_format='parquet'
    )
}}

with orders as (
    select *
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where _ingestion_ts > (select coalesce(max(_ingestion_ts_orders), timestamp '1970-01-01') from {{ this }})
    {% endif %}
),

payments as (
    select *
    from {{ ref('stg_payments') }}
),

shipments as (
    select *
    from {{ ref('stg_shipments') }}
),

-- Get the best payment record for each order (prefer successful payments, then latest)
ranked_payments as (
    select
        *,
        row_number() over (
            partition by order_id
            order by
                case when payment_status = 'completed' then 0 else 1 end,
                payment_date desc nulls last
        ) as payment_rank
    from payments
),

latest_payments as (
    select *
    from ranked_payments
    where payment_rank = 1
),

-- Get the latest shipment record for each order
ranked_shipments as (
    select
        *,
        row_number() over (
            partition by order_id
            order by
                coalesce(actual_delivery_date, shipment_date, created_at) desc nulls last
        ) as shipment_rank
    from shipments
),

latest_shipments as (
    select *
    from ranked_shipments
    where shipment_rank = 1
),

order_facts as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.payment_status,
        o.payment_method,
        coalesce(p.amount, o.total_amount) as payment_amount,
        coalesce(p.payment_date, o.payment_date) as payment_date,
        s.status as shipment_status,
        coalesce(s.shipping_carrier, o.shipping_carrier) as shipping_carrier,
        coalesce(cast(s.tracking_number as varchar), o.tracking_number) as tracking_number,
        s.shipment_date,
        s.estimated_delivery_date,
        s.actual_delivery_date,
        o.subtotal,
        o.discount_amount,
        o.tax_amount,
        o.shipping_cost,
        o.total_amount,
        o._ingestion_ts as _ingestion_ts_orders,
        date(o.order_date) as order_date_partition

    from orders o
    left join latest_payments p on o.order_id = p.order_id
    left join latest_shipments s on o.order_id = s.order_id
)

select * from order_facts

