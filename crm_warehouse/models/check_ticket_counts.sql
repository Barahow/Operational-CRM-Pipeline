{{ config(
    materialized='incremental',
    schema='crm_warehouse_transformed',
    alias='fact_support_ticket_transformed',
    unique_key='ticket_id'
) }}

with raw as (
    select
        ticket_id,
        customer_id,
        customer_email,
        ticket_subject,
        ticket_description,
        ticket_status,
        resolution,
        ticket_priority,
        ticket_channel,
        cast(first_response_time as timestamp) as first_response_time,
        cast(ticket_created as timestamp) as ticket_created,
        cast(frt_minutes as numeric) as frt_minutes,
        cast(ttr_minutes as string) as ttr_minutes,
        cast(customer_satisfaction as numeric) as customer_satisfaction
    from {{ source('crm_warehouse','fact_support_ticket') }}
)

select *
from raw

{% if is_incremental() %}
  where ticket_created > (
    select coalesce(max(ticket_created), timestamp('1970-01-01')) from {{ this }}
  )
{% endif %}

