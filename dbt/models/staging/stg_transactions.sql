-- Staging layer: 1:1 with the raw source, light typing/renaming only.
-- No business logic here - that belongs in intermediate/marts.

with source as (
    select * from {{ source('raw', 'stg_transactions') }}
)

select
    transaction_id,
    customer_id,
    product_id,
    store_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    cast(transaction_timestamp as timestamp_tz) as transaction_timestamp,
    transaction_type,
    date(transaction_timestamp)                 as transaction_date
from source
where transaction_id is not null   -- defence in depth; Spark should already filter these
