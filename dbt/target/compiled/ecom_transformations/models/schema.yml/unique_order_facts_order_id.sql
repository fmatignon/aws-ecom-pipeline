
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "AwsDataCatalog"."ecom_silver_ecom_gold"."order_facts"
where order_id is not null
group by order_id
having count(*) > 1


