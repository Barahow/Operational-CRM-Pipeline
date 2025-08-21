{{ config(
    materialized='incremental',
    schema='crm_warehouse_transformed',
    alias='fact_support_ticket_transformed',
    unique_key='ticket_id'
) }}

with raw as (
  select * 
  from {{ source('crm_warehouse','fact_support_ticket') }}
)

select
  ticket_id,
  customer_name,
  customer_email,
  customer_age,
  customer_gender,
  product_purchased,
  cast(date_of_purchase as date) as date_of_purchase,
  ticket_type,
  ticket_subject,
  ticket_description,
  ticket_status,
  resolution,
  ticket_priority,
  ticket_channel,
  first_response_time,
  time_to_resolution,
  customer_satisfaction
from raw

{% if is_incremental() %}
  where cast(date_of_purchase as date) > (
    select coalesce(max(date_of_purchase), '1970-01-01') from {{ this }}
  )
{% endif %}

