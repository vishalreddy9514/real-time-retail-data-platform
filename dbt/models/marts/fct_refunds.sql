-- Grain: one row per refund transaction (subset of fct_sales where
-- transaction_type = 'refund').

select
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as refund_sk,
    transaction_id,
    customer_id,
    product_id,
    abs(total_amount) as refund_amount,
    transaction_date
from {{ ref('stg_transactions') }}
where transaction_type = 'refund'
