-- Custom singular dbt test: for any given product, cumulative refunded
-- amount should never exceed cumulative gross revenue - if it does, that
-- signals a data integrity bug upstream (e.g. duplicate refund events)
-- rather than a legitimate business scenario. dbt test convention: a
-- query that returns rows = failure.

select
    product_id,
    gross_revenue,
    refunded_amount
from {{ ref('dim_product') }}
where refunded_amount > gross_revenue
