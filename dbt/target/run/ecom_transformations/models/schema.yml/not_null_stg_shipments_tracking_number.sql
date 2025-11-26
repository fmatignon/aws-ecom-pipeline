
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tracking_number
from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_shipments"
where tracking_number is null



  
  
      
    ) dbt_internal_test