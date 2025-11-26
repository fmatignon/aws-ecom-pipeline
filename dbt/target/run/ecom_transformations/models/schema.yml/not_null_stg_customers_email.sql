
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_customers"
where email is null



  
  
      
    ) dbt_internal_test