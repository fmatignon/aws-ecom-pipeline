{#
"""
Gold model for daily business metrics aggregated from order_facts.
Provides daily KPIs including total orders, revenue, and delivery performance.
"""
#}

{{
    config(
        materialized='incremental',
        unique_key='day',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partitioned_by=['day'],
        file_format='parquet'
    )
}}

with order_facts as (
    select *
    from {{ ref('order_facts') }}
    {% if is_incremental() %}
    -- Refresh last 7 days to catch late-arriving data
    where date(order_date) >= date_add('day', -7, current_date)
    {% endif %}
),

daily_aggregates as (
    select
        -- Order counts
        count(distinct order_id) as total_orders,

        -- Revenue metrics
        coalesce(sum(total_amount), 0) as total_revenue,

        -- Average order value
        case 
            when count(distinct order_id) > 0 
            then sum(total_amount) / count(distinct order_id)
            else 0 
        end as avg_order_value,

        -- Payment status metrics
        count(distinct case when payment_status in ('completed', 'success') then order_id end) as paid_orders,

        -- Delivery metrics
        count(distinct case 
            when shipment_status = 'delivered' or actual_delivery_date is not null 
            then order_id 
        end) as delivered_orders,

        -- Refund amounts (sum of negative total_amount or refunded orders)
        coalesce(sum(case 
            when total_amount < 0 then total_amount
            when payment_status = 'refunded' then -1 * total_amount
            else 0 
        end), 0) as refund_amounts,

        -- Partition column must be last
        date(order_date) as day

    from order_facts
    where order_date is not null
    group by date(order_date)
)

select * from daily_aggregates

