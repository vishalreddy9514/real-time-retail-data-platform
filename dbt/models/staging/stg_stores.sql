with source as (
    select * from {{ source('raw', 'stg_stores') }}
)

select
    store_id,
    store_name,
    city,
    region,
    latitude,
    longitude
from source
where store_id is not null
