-- Grain: one row per payment associated with a transaction. In this
-- simulation payment and transaction are 1:1, but modelling it as its own
-- fact table keeps the door open for multi-attempt/split payments later
-- without reshaping fct_sales.

select
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as payment_sk,
    transaction_id,
    payment_method,
    total_amount as amount,
    transaction_date
from {{ ref('stg_transactions') }}
