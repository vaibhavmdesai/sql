-- Databricks notebook source
-- MAGIC %md
-- MAGIC Customer retention refers to the ability of a company or product to retain its customers over some specified period. High customer retention means customers of the product or business tend to return to, continue to buy or in some other way not defect to another product or business, or to non-use entirely. 
-- MAGIC Company programs to retain customers: Zomato Pro , Cashbacks, Reward Programs etc.
-- MAGIC Once these programs in place we need to build metrics to check if programs are working or not. That is where we will write SQL to drive customer retention count.  

-- COMMAND ----------

create table sales_transactions(
order_id int,
cust_id int,
order_date date,
amount int
);
delete from sales_transactions;
insert into sales_transactions values 
(1,1,'2020-01-15',150)
,(2,1,'2020-02-10',150)
,(3,2,'2020-01-16',150)
,(4,2,'2020-02-25',150)
,(5,3,'2020-01-10',150)
,(6,3,'2020-02-20',150)
,(7,4,'2020-01-20',150)
,(8,5,'2020-02-20',150)
;

-- COMMAND ----------

select * from sales_transactions

-- COMMAND ----------

WITH cte AS (
    SELECT cust_id,
        DATE_ADD(LAST_DAY(ADD_MONTHS(order_date, -1)), 1) AS order_month,
        ADD_MONTHS(order_month, -1) AS order_prev_month
    FROM sales_transactions
    ORDER BY order_month,
        cust_id
)
SELECT this_month.order_month,
    Count(DISTINCT last_month.cust_id) AS cust_count
FROM cte this_month
    LEFT JOIN cte last_month ON this_month.cust_id = last_month.cust_id
    AND this_month.order_prev_month = last_month.order_month
GROUP BY this_month.order_month
ORDER BY this_month.order_month

-- COMMAND ----------

WITH cte AS (
    SELECT cust_id,
        DATE_ADD(LAST_DAY(ADD_MONTHS(order_date, -1)), 1) AS order_month,
        ADD_MONTHS(order_month, 1) AS order_next_month
    FROM sales_transactions
    ORDER BY order_month,
        cust_id
)
SELECT this_month.order_month, this_month.cust_id, last_month.order_next_month, last_month.cust_id
FROM cte last_month
    LEFT JOIN cte this_month ON this_month.cust_id = last_month.cust_id
    AND this_month.order_month = last_month.order_next_month
    
-- SELECT last_month.order_month,
--     Count(1) AS cust_count
-- FROM cte this_month
--     RIGHT JOIN cte last_month ON this_month.cust_id = last_month.cust_id
--     AND this_month.order_month = last_month.order_next_month
-- GROUP BY this_month.order_month
-- ORDER BY this_month.order_month
