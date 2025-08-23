-- models/dim_tickets.sql
with raw as (
    select * from {{ source('crm_warehouse','dim_ticket') }}
)

select
    ticket_id,
    ticket_type,
    ticket_channel,
    ticket_status,
    ticket_priority,
    first_response_time,
    time_to_resolution,
    customer_id
from raw
