# Power BI Dashboard Specification

This project's BI layer connects Power BI directly to the Snowflake
`ANALYTICS` schema (marts built by dbt) using the native Snowflake
connector - **never** the raw Kafka stream or the S3 raw zone. This
keeps the dashboard fast, stable, and decoupled from pipeline internals.

Connect: Power BI Desktop → Get Data → Snowflake → `RETAIL_PLATFORM.ANALYTICS`
→ Import or DirectQuery on `fct_sales`, `fct_payments`, `fct_refunds`,
`dim_customer`, `dim_product`, `dim_store`, `dim_date`, `dim_time`.

## Core DAX measures

```
Total Sales := SUM(fct_sales[total_amount])
Total Transactions := COUNTROWS(FILTER(fct_sales, fct_sales[transaction_type] = "purchase"))
Average Transaction Value := DIVIDE([Total Sales], [Total Transactions])
Refund Rate := DIVIDE(
    COUNTROWS(FILTER(fct_sales, fct_sales[transaction_type] = "refund")),
    COUNTROWS(fct_sales)
)
Top Product := TOPN(1, VALUES(dim_product[product_name]), [Total Sales], DESC)
Repeat Customer Rate := DIVIDE(
    COUNTROWS(FILTER(dim_customer, dim_customer[is_repeat_customer] = TRUE)),
    COUNTROWS(dim_customer)
)
```

## Page 1 — Executive Overview
- KPI cards: Total Sales, Total Transactions, Average Transaction Value, Refund Rate
- Top Product / Top City cards (using the measures above sliced by dim_product/dim_store)
- Line chart: Total Sales trend by `dim_date[full_date]`

## Page 2 — Sales Analysis
- Column chart: Sales by day (`dim_date`) and by hour (`dim_time[hour]`)
- Map/bar chart: Sales by city (`dim_store[city]`)
- Stacked bar: Sales by category (`dim_product[category]`)

## Page 3 — Product Analysis
- Table: Top products by revenue and units sold (`fct_sales` × `dim_product`)
- Treemap: Revenue by category/subcategory
- Bar: Units sold by brand

## Page 4 — Customer Analysis
- Donut: Customers by `customer_segment`
- Card: Repeat Customer Rate
- Bar: Average basket value by segment (`dim_customer[avg_basket_value]`)

## Page 5 — Anomaly Monitoring
- Table: flagged transactions from the `anomalies` curated table (anomaly_type,
  customer_id, detected_at, detail, severity)
- Bar: anomaly count by `anomaly_type`
- Line: anomaly trend over time (daily count by type)

## Refresh strategy
- DirectQuery for the Anomaly Monitoring page (near-real-time expectations)
- Scheduled Import refresh (every 15–30 min) for the other pages, aligned
  with the `dbt_transformation` Airflow DAG's cadence
