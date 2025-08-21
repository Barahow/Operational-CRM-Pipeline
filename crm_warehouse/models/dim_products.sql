{{ config(
    materialized='table',
    schema='crm_warehouse',
    alias='dim_products'
) }}

with raw as (
    select * 
    from {{ source('crm_warehouse','dim_product') }}
)

select
    product_id,
    product_name,
    brand,
    category
from raw
where product_id is not null
