
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        payment_status as value_field,
        count(*) as n_records

    from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_orders"
    group by payment_status

)

select *
from all_values
where value_field not in (
    'completed','failed','pending','refunded'
)



  
  
      
    ) dbt_internal_test