-- Grain: one row per store.

select
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_sk,
    store_id,
    store_name,
    city,
    region,
    latitude,
    longitude
from {{ ref('stg_stores') }}
