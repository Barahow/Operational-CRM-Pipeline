with config as (
    select 1 as dummy -- dummy select to anchor SQLFluff parsing
)

{{ config(
    materialized='table',
    schema='crm_warehouse',
    alias='dim_customers'
) }}

, raw as (
    select *
    from {{ source('crm_warehouse','dim_customers') }}
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
    income_bracket,
    signup_date
from raw
where customer_id is not null
