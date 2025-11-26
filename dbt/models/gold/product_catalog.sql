{#
"""
Gold model for the product catalog providing a clean, deduplicated view of all
products with standardized attributes for reporting and analytics.
"""
#}

{{
    config(
        materialized='table',
        file_format='parquet'
    )
}}

with products as (
    select *
    from {{ ref('stg_products') }}
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

