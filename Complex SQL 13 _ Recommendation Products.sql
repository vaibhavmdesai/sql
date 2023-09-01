-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC Query to find products which are most frequently bought together using simple SQL. Based on the history ecommerce website can recommend products to new user.
-- MAGIC
-- MAGIC https://www.youtube.com/watch?v=9Kh7EnZlhUg&list=PLBTZqjSKn0IeKBQDjLmzisazhqQy4iGkb&index=13

-- COMMAND ----------

create table orders
(
order_id int,
customer_id int,
product_id int
);

insert into orders VALUES 
(1, 1, 1),
(1, 1, 2),
(1, 1, 3),
(2, 2, 1),
(2, 2, 2),
(2, 2, 4),
(3, 1, 5);

create table products (
id int,
name varchar(10)
);
insert into products VALUES 
(1, 'A'),
(2, 'B'),
(3, 'C'),
(4, 'D'),
(5, 'E');

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select concat_ws(' ', p1.name, p2.name) as pair, count(1) as purchase_freq
from orders o1 
inner join orders o2 on o1.order_id = o2.order_id and o1.product_id < o2.product_id
inner join products p1 on o1.product_id = p1.id
inner join products p2 on o2.product_id = p2.id
group by pair
