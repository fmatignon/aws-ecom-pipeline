
    
    

select
    day as unique_field,
    count(*) as n_records

from "AwsDataCatalog"."ecom_silver_ecom_gold"."daily_metrics"
where day is not null
group by day
having count(*) > 1


