-- Grain: one row per product.

with products as (
    select * from {{ ref('stg_products') }}
),

sales as (
    select * from {{ ref('int_product_sales') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['products.product_id']) }} as product_sk,
    products.product_id,
    products.product_name,
    products.category,
    products.subcategory,
    products.brand,
    products.price,
    products.cost,
    coalesce(sales.units_sold, 0)       as units_sold,
    coalesce(sales.gross_revenue, 0)    as gross_revenue,
    coalesce(sales.refunded_amount, 0)  as refunded_amount,
    coalesce(sales.refund_count, 0)     as refund_count
from products
left join sales on products.product_id = sales.product_id
