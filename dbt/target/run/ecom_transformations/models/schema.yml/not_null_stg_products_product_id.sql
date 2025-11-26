
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_products"
where product_id is null



  
  
      
    ) dbt_internal_test