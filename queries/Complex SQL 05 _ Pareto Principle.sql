-- Databricks notebook source
-- MAGIC %py
-- MAGIC
-- MAGIC spark.read.option('inferSchema', True).option('header', True).csv('/FileStore/tables/Superstore_orders.csv').createOrReplaceTempView('cust_orders')

-- COMMAND ----------

select sum(sales)*0.8 from cust_orders

-- COMMAND ----------

WITH cte
     AS (SELECT 
                Product_Id,
                Sum(sales)                     AS sales
         FROM   cust_orders
         GROUP  BY 
                   Product_Id)
     ,rolling_sales
     AS (SELECT *,
                Sum(sales)
                  OVER(
                    ORDER BY sales desc ROWS BETWEEN unbounded
                  preceding
                  AND
                  CURRENT ROW) AS rolling_sum,
                0.8 * Sum(sales)
                        OVER() AS total_sales
         FROM   cte)
SELECT *
FROM   rolling_sales
WHERE  rolling_sum <= total_sales 

-- COMMAND ----------

select 14161.348999999998+4119.816+55526.19900000002
