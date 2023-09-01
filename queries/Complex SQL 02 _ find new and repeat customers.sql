-- Databricks notebook source
-- DBTITLE 1,Question
CREATE TABLE customer_orders (
  order_id integer,
  customer_id integer,
  order_date date,
  order_amount integer
);

INSERT INTO customer_orders
  VALUES (1, 100, CAST('2022-01-01' AS date), 2000), (2, 200, CAST('2022-01-01' AS date), 2500), (3, 300, CAST('2022-01-01' AS date), 2100)
  , (4, 100, CAST('2022-01-02' AS date), 2000), (5, 400, CAST('2022-01-02' AS date), 2200), (6, 500, CAST('2022-01-02' AS date), 2700)
  , (7, 100, CAST('2022-01-03' AS date), 3000), (8, 400, CAST('2022-01-03' AS date), 1000), (9, 600, CAST('2022-01-03' AS date), 3000)
;


SELECT * FROM customer_orders;

-- COMMAND ----------

-- DBTITLE 1,Solution
WITH first_visit_cte
AS (SELECT
  customer_id,
  MIN(order_date) AS first_visit_date
FROM customer_orders
GROUP BY customer_id)
,repeat_cte
AS (SELECT
  co.customer_id,
  co.order_date,
  CASE
    WHEN co.order_date > fv.first_visit_date THEN 1
    ELSE 0
  END repeat_customer_flag
FROM customer_orders co
INNER JOIN first_visit_cte fv
  ON co.customer_id = fv.customer_id)
SELECT
  order_date,
  SUM(CASE
    WHEN repeat_customer_flag = 0 THEN 1
    ELSE 0
  END) AS no_new_customers,
  SUM(CASE
    WHEN repeat_customer_flagrepeat_customer = 1 THEN 1
    ELSE 0
  END) AS no_repeat_customers
FROM repeat_cte
GROUP BY order_date
ORDER BY 1

-- COMMAND ----------

-- DBTITLE 1,With Order amount
WITH first_visit_cte
AS (SELECT
  customer_id,
  MIN(order_date) AS first_visit_date
FROM customer_orders
GROUP BY customer_id)
,repeat_cte
AS (SELECT
  co.customer_id,
  co.order_date,
  co.order_amount,
  CASE
    WHEN co.order_date > fv.first_visit_date THEN 1
    ELSE 0
  END repeat_customer_flag
FROM customer_orders co
INNER JOIN first_visit_cte fv
  ON co.customer_id = fv.customer_id)
SELECT
order_date,
SUM(CASE
    WHEN repeat_customer_flag = 0 THEN 1
    ELSE 0
  END) AS no_new_customers,
SUM(CASE
    WHEN repeat_customer_flag = 0 THEN order_amount
  END) AS new_customer_amount,
SUM(CASE
    WHEN repeat_customer_flag = 1 THEN 1
    ELSE 0
  END) AS no_repeat_customers,
SUM(CASE
    WHEN repeat_customer_flag = 1 THEN order_amount
  END) AS repeat_customer_amount
FROM repeat_cte
GROUP BY order_date
ORDER BY 1
