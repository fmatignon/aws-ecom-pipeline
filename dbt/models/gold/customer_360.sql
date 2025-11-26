{#
"""
Gold model for customer 360 view at the grain of customer_id. Aggregates order
history and preferences for each customer, providing a comprehensive view of
customer behavior and value.
"""
#}

{{
    config(
        materialized='table',
        file_format='parquet'
    )
}}

with customers as (
    select
        customer_id,
        email
    from {{ ref('stg_customers') }}
),

order_facts as (
    select *
    from {{ ref('order_facts') }}
),

-- Aggregate order metrics per customer
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct order_id) as total_orders,
        coalesce(sum(total_amount), 0) as total_revenue,
        case 
            when count(distinct order_id) > 0 
            then sum(total_amount) / count(distinct order_id)
            else 0 
        end as avg_order_value
    from order_facts
    group by customer_id
),

-- Get preferred payment method (mode with tie-breaker on latest order)
payment_method_counts as (
    select
        customer_id,
        payment_method,
        count(*) as method_count,
        max(order_date) as latest_use
    from order_facts
    where payment_method is not null
    group by customer_id, payment_method
),

ranked_payment_methods as (
    select
        customer_id,
        payment_method,
        row_number() over (
            partition by customer_id 
            order by method_count desc, latest_use desc
        ) as method_rank
    from payment_method_counts
),

preferred_payment as (
    select customer_id, payment_method as preferred_payment_method
    from ranked_payment_methods
    where method_rank = 1
),

-- Get preferred shipping carrier (mode with tie-breaker on latest order)
carrier_counts as (
    select
        customer_id,
        shipping_carrier,
        count(*) as carrier_count,
        max(order_date) as latest_use
    from order_facts
    where shipping_carrier is not null
    group by customer_id, shipping_carrier
),

ranked_carriers as (
    select
        customer_id,
        shipping_carrier,
        row_number() over (
            partition by customer_id 
            order by carrier_count desc, latest_use desc
        ) as carrier_rank
    from carrier_counts
),

preferred_carrier as (
    select customer_id, shipping_carrier as preferred_shipping_carrier
    from ranked_carriers
    where carrier_rank = 1
),

customer_360 as (
    select
        c.customer_id,
        c.email,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.total_revenue, 0) as total_revenue,
        coalesce(co.avg_order_value, 0) as avg_order_value,
        case 
            when co.last_order_date is not null 
            then date_diff('day', date(co.last_order_date), current_date)
            else null 
        end as days_since_last_order,
        pp.preferred_payment_method,
        pc.preferred_shipping_carrier

    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
    left join preferred_payment pp on c.customer_id = pp.customer_id
    left join preferred_carrier pc on c.customer_id = pc.customer_id
)

select * from customer_360

