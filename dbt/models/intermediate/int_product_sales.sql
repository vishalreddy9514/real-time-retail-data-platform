-- Ephemeral intermediate model: aggregates sales performance per product,
-- separating purchases from refunds so net revenue can be derived cleanly
-- in the marts layer.

with transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    product_id,
    sum(case when transaction_type = 'purchase' then quantity else 0 end)      as units_sold,
    sum(case when transaction_type = 'purchase' then total_amount else 0 end)  as gross_revenue,
    sum(case when transaction_type = 'refund' then abs(total_amount) else 0 end) as refunded_amount,
    count(case when transaction_type = 'refund' then 1 end)                    as refund_count
from transactions
group by product_id
