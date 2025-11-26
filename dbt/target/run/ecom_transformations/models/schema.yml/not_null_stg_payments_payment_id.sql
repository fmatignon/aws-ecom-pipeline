
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_id
from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_payments"
where payment_id is null



  
  
      
    ) dbt_internal_test