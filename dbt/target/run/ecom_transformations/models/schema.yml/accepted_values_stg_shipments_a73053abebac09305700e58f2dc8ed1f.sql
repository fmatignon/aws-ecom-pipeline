
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_shipments"
    group by status

)

select *
from all_values
where value_field not in (
    'pending','in_transit','delivered','returned','cancelled','exception'
)



  
  
      
    ) dbt_internal_test