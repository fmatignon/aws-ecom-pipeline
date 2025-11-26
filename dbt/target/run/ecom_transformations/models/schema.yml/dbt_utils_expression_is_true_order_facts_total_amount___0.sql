
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with meet_condition as (
    select * from "AwsDataCatalog"."ecom_silver_ecom_gold"."order_facts" where 1=1
)

select
    *
from meet_condition

where not(total_amount >= 0)


  
  
      
    ) dbt_internal_test