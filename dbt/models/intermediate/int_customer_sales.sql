-- Ephemeral intermediate model: aggregates purchase behaviour per customer.
-- Feeds dim_customer enrichment (repeat customer flag, basket value) in marts.

with transactions as (
    select * from {{ ref('stg_transactions') }}
    where transaction_type = 'purchase'
)

select
    customer_id,
    count(distinct transaction_id)              as total_purchases,
    sum(total_amount)                            as lifetime_spend,
    avg(total_amount)                            as avg_basket_value,
    min(transaction_date)                        as first_purchase_date,
    max(transaction_date)                        as last_purchase_date,
    case when count(distinct transaction_id) > 1 then true else false end as is_repeat_customer
from transactions
group by customer_id
