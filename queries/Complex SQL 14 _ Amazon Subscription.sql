-- Databricks notebook source
-- MAGIC %md
-- MAGIC Given the following two tables, return the fraction of users, rounded to two decimal places,
-- MAGIC who accessed Amazon music and upgraded to prime membership within the first 30 days of signing up. 

-- COMMAND ----------

create table ausers
(
user_id integer,
name varchar(20),
join_date date
);

insert into ausers
values (1, 'Jon', CAST('2020-02-14' AS date)), 
(2, 'Jane', CAST('2020-02-14' AS date)), 
(3, 'Jill', CAST('2020-02-15' AS date)), 
(4, 'Josh', CAST('2020-02-15' AS date)), 
(5, 'Jean', CAST('2020-02-16' AS date)), 
(6, 'Justin', CAST('2020-02-17' AS date)),
(7, 'Jeremy', CAST('2020-02-18' AS date));

create table events
(
user_id integer,
type varchar(10),
access_date date
);

insert into events values
(1, 'Pay', CAST('2020-03-01' AS date)), 
(2, 'Music', CAST('2020-03-02' AS date)), 
(2, 'P', CAST('2020-03-12' AS date)),
(3, 'Music', CAST('2020-03-15' AS date)), 
(4, 'Music', CAST('2020-03-15' AS date)), 
(1, 'P', CAST('2020-03-16' AS date)), 
(3, 'P', CAST('2020-03-22' AS date));

-- COMMAND ----------

select * from ausers

-- COMMAND ----------

select * from events

-- COMMAND ----------

with music_users
as
(
select * from ausers where user_id in (select user_id from events where type = 'Music')
)
select round(count(e.access_date) / count(1), 2) as fraction
from music_users mu left join events e
on mu.user_id = e.user_id
and e.type = 'P'
and months_between(e.access_date, mu.join_date) < 1
