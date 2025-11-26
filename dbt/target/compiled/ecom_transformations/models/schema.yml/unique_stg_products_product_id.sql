
    
    

select
    product_id as unique_field,
    count(*) as n_records

from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_products"
where product_id is not null
group by product_id
having count(*) > 1


