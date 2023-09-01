-- Databricks notebook source
-- MAGIC %md
-- MAGIC Write an SQL query to find for each user, whether the brand of the second item (by date) they sold is their favorite brand. 
-- MAGIC If a user sold less than two items, report the answer for that user as no.
-- MAGIC
-- MAGIC It is guaranteed that no seller sold more than one item on a day.

-- COMMAND ----------

-- create table users1 (
-- user_id         int     ,
--  join_date       date    ,
--  favorite_brand  varchar(50));

 create table orders (
 order_id       int     ,
 order_date     date    ,
 item_id        int     ,
 buyer_id       int     ,
 seller_id      int 
 );

 create table items
 (
 item_id        int     ,
 item_brand     varchar(50)
 );


 insert into users1 values (1,'2019-01-01','Lenovo'),(2,'2019-02-09','Samsung'),(3,'2019-01-19','LG'),(4,'2019-05-21','HP');

 insert into items values (1,'Samsung'),(2,'Lenovo'),(3,'LG'),(4,'HP');

 insert into orders values (1,'2019-08-01',4,1,2),(2,'2019-08-02',2,1,3),(3,'2019-08-03',3,2,3),(4,'2019-08-04',1,4,2)
 ,(5,'2019-08-04',1,3,4),(6,'2019-08-05',2,2,4);

-- COMMAND ----------

select * from users1

-- COMMAND ----------

select * from items

-- COMMAND ----------

select * from orders

-- COMMAND ----------

-- DBTITLE 1,Check the number of items sold by each seller
WITH 
cte_items_sold AS (
    SELECT seller_id,
        COUNT(DISTINCT item_id) AS items_sold
    FROM orders
    GROUP BY seller_id
)
,cte_second_item AS (
    SELECT a.item_id AS second_item_id,
        it.item_brand AS second_item_brand,
        a.seller_id
    FROM (
            SELECT *,
                ROW_NUMBER() OVER(
                    PARTITION BY seller_id
                    ORDER BY order_date ASC
                ) AS rn
            FROM orders
        ) a
        INNER JOIN cte_items_sold i ON i.seller_id = a.seller_id
        INNER JOIN items it ON it.item_id = a.item_id
    WHERE a.rn = 2
        AND i.items_sold > 1
)
SELECT u.user_id AS seller_id,
    CASE
        WHEN u.favorite_brand = c.second_item_brand THEN 'Yes'
        ELSE 'No'
    END 2nd_item_fav_brand
FROM users1 u
    LEFT JOIN cte_second_item c ON u.user_id = c.seller_id

-- COMMAND ----------

WITH cte_second_item AS (
    SELECT a.item_id AS second_item_id,
        it.item_brand AS second_item_brand,
        a.seller_id
    FROM (
            SELECT *,
                ROW_NUMBER() OVER(
                    PARTITION BY seller_id
                    ORDER BY order_date ASC
                ) AS rn
            FROM orders
        ) a
        INNER JOIN items it ON it.item_id = a.item_id
        AND a.rn = 2
)
SELECT u.user_id AS seller_id,
CASE
        WHEN u.favorite_brand = c.second_item_brand THEN 'Yes'
        ELSE 'No'
    END 2nd_item_fav_brand
FROM users1 u
    LEFT JOIN cte_second_item c ON u.user_id = c.seller_id
