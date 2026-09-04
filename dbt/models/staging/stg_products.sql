with source as (
    select * from {{ source('raw', 'stg_products') }}
)

select
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    price,
    cost,
    stock_quantity
from source
where product_id is not null
