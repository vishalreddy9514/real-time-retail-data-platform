with source as (
    select * from {{ source('raw', 'stg_customers') }}
)

select
    customer_id,
    customer_name,
    age_group,
    postcode_area,
    customer_segment,
    cast(registration_date as date) as registration_date
from source
where customer_id is not null
