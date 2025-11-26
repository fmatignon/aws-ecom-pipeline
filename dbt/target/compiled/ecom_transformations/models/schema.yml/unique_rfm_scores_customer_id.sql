
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from "AwsDataCatalog"."ecom_silver_ecom_gold"."rfm_scores"
where customer_id is not null
group by customer_id
having count(*) > 1


