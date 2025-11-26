

with meet_condition as (
    select * from "AwsDataCatalog"."ecom_silver_ecom_gold"."order_facts" where 1=1
)

select
    *
from meet_condition

where not(total_amount >= 0)

