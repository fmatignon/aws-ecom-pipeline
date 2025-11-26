



with order_facts as (
    select *
    from "AwsDataCatalog"."ecom_silver_ecom_gold"."order_facts"
    
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