-- Databricks notebook source
-- MAGIC %md
-- MAGIC In this video we will silve a leetcode hard problem 1369 . Where we need to find second most recent activity and if user has only 1 activoty then return that as it is. We will use SQL window functions to solve this problem.

-- COMMAND ----------

create table UserActivity
(
username      varchar(20) ,
activity      varchar(20),
startDate     Date   ,
endDate      Date
);

insert into UserActivity values 
('Alice','Travel','2020-02-12','2020-02-20')
,('Alice','Dancing','2020-02-21','2020-02-23')
,('Alice','Travel','2020-02-24','2020-02-28')
,('Bob','Travel','2020-02-11','2020-02-18');

-- COMMAND ----------

select a.username, a.activity,a.startdate,a.enddate from(
select *
, RANK() OVER(PARTITION BY username order by startdate) as act_no
, COUNT(1) OVER(PARTITION BY username order by username) as act_cnt 
from UserActivity)a
where a.act_no=2 or a.act_cnt=1
