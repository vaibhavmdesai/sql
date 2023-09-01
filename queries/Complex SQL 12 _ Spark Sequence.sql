-- Databricks notebook source
-- MAGIC %md
-- MAGIC Calculate total sales for year for every product

-- COMMAND ----------

create table sales (
  product_id int,
  period_start date,
  period_end date,
  average_daily_sales int
);
insert into
  sales
values(1, '2019-01-25', '2019-02-28', 100),(2, '2018-12-01', '2020-01-01', 10),(3, '2019-12-01', '2020-01-31', 1);
select
  *
from
  sales;

-- COMMAND ----------

with cte as (
  select
    product_id,
    explode(
      sequence(
        to_date(period_start),
        to_date(period_end),
        interval 1 day
      )
    ) as dates,
    average_daily_sales
  from
    sales
)
select
  product_id,
  extract(
    year
    from
      dates
  ) as reporting_year,
  sum(average_daily_sales) as total_sales
from
  cte
group by
  product_id,
  reporting_year
order by
  product_id,
  reporting_year
