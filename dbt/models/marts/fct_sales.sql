-- Grain: ONE ROW PER TRANSACTION (purchase, refund, or cancellation).
-- This is the primary fact table the Power BI "Sales Analysis" and
-- "Executive Overview" pages query against.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select customer_sk, customer_id from {{ ref('dim_customer') }}
),

products as (
    select product_sk, product_id from {{ ref('dim_product') }}
),

stores as (
    select store_sk, store_id from {{ ref('dim_store') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['transactions.transaction_id']) }} as transaction_sk,
    transactions.transaction_id,
    customers.customer_sk,
    products.product_sk,
    stores.store_sk,
    transactions.transaction_date,
    transactions.transaction_timestamp,
    transactions.quantity,
    transactions.unit_price,
    transactions.total_amount,
    transactions.payment_method,
    transactions.transaction_type
from transactions
left join customers on transactions.customer_id = customers.customer_id
left join products  on transactions.product_id  = products.product_id
left join stores    on transactions.store_id    = stores.store_id
