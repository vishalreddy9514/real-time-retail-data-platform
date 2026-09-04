-- Grain: one row per customer. Type-1 SCD (current attributes only),
-- enriched with derived purchase-behaviour metrics from int_customer_sales.

with customers as (
    select * from {{ ref('stg_customers') }}
),

sales as (
    select * from {{ ref('int_customer_sales') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customers.customer_id']) }} as customer_sk,
    customers.customer_id,
    customers.customer_name,
    customers.age_group,
    customers.postcode_area,
    customers.customer_segment,
    customers.registration_date,
    coalesce(sales.total_purchases, 0)      as total_purchases,
    coalesce(sales.lifetime_spend, 0)       as lifetime_spend,
    sales.avg_basket_value,
    coalesce(sales.is_repeat_customer, false) as is_repeat_customer
from customers
left join sales on customers.customer_id = sales.customer_id
