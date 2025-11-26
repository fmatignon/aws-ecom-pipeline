



with products as (
    select *
    from "AwsDataCatalog"."ecom_silver_ecom_silver"."stg_products"
),

product_catalog as (
    select
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        price,
        cost,
        created_at,
        color
    from products
)

select * from product_catalog