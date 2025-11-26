
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_orders"
where order_id is not null
group by order_id
having count(*) > 1


