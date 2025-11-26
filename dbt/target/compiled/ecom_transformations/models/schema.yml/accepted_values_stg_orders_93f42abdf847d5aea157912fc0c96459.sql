
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_orders"
    group by order_status

)

select *
from all_values
where value_field not in (
    'processing','shipped','delivered','cancelled','pending','refunded'
)


