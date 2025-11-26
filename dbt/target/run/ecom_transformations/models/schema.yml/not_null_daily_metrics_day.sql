
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select day
from "AwsDataCatalog"."ecom_silver_ecom_gold"."daily_metrics"
where day is null



  
  
      
    ) dbt_internal_test