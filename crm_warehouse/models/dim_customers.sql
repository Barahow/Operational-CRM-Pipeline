{{ config(
    materialized='table',
    schema='crm_warehouse',
    alias='dim_customers'
) }}

with raw as (
    select *
    from {{ source('crm_warehouse','dim_customer') }}
)

select
    customer_id,
    customer_name,
    customer_email,
    customer_age,
    customer_gender,
    first_ticket_date,
    last_ticket_date,
    location,
    account_age_years,
    income_bracket
from raw
where customer_id is not null
