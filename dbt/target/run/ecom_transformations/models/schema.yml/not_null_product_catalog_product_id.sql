
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from "AwsDataCatalog"."ecom_silver_ecom_gold"."product_catalog"
where product_id is null



  
  
      
    ) dbt_internal_test