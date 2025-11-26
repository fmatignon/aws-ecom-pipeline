
    
    

select
    tracking_number as unique_field,
    count(*) as n_records

from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_shipments"
where tracking_number is not null
group by tracking_number
having count(*) > 1


