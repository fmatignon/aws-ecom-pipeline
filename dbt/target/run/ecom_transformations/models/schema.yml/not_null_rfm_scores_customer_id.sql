
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from "AwsDataCatalog"."ecom_silver_ecom_gold"."rfm_scores"
where customer_id is null



  
  
      
    ) dbt_internal_test