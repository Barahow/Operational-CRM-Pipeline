{{ config(
    materialized='incremental',
    schema='crm_warehouse_transformed',
    alias='fact_sales_transformed',
    unique_key='sale_id',
    partition_by={'field':'purchase_date','data_type':'date'},
    cluster_by=['product_id','customer_id']
) }}

with raw as (
    select * from {{ source('crm_warehouse','fact_sales') }}
)

select
    sale_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    CAST(purchase_date as DATE) as purchase_date
from raw

{% if is_incremental() %}
    where
        CAST(purchase_date as DATE)
        > (select COALESCE(MAX(purchase_date), '1970-01-01') from {{ this }})
{% endif %}
