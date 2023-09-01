-- Databricks notebook source
-- MAGIC %md
-- MAGIC The table logs the spendings history of users that make purchases from an online shopping website which has a desktop 
-- MAGIC and a mobile application.
-- MAGIC
-- MAGIC Write an SQL query to find the total number of users and the total amount spent using mobile only, desktop only and both mobile and desktop together for each date.
-- MAGIC The query result format is in the following example:

-- COMMAND ----------

create table spending 
(
user_id int,
spend_date date,
platform varchar(10),
amount int
);

insert into spending values(1,'2019-07-01','mobile',100),(1,'2019-07-01','desktop',100),(2,'2019-07-01','mobile',100)
,(2,'2019-07-02','mobile',100),(3,'2019-07-01','desktop',100),(3,'2019-07-02','desktop',100);

-- COMMAND ----------

select * from spending

-- COMMAND ----------

select * from spending
where platform = 'mobile'

-- COMMAND ----------

select * from spending
where platform = 'desktop'

-- COMMAND ----------

-- DBTITLE 1,mobile only
Select m.* from
(select * from spending
where platform = 'mobile') m
left join 
(select * from spending
where platform = 'desktop') d
on m.user_id = d.user_id
and m.spend_date = d.spend_date
where d.user_id is null

-- COMMAND ----------

-- DBTITLE 1,desktop only
Select d.* from
(select * from spending
where platform = 'mobile') m
right join 
(select * from spending
where platform = 'desktop') d
on m.user_id = d.user_id
and m.spend_date = d.spend_date
where m.user_id is null

-- COMMAND ----------

-- DBTITLE 1,My Solution
WITH all_cte
     AS (SELECT m.*
         FROM   (SELECT *
                 FROM   spending
                 WHERE  platform = 'mobile') m
                LEFT JOIN (SELECT *
                           FROM   spending
                           WHERE  platform = 'desktop') d
                       ON m.user_id = d.user_id
                          AND m.spend_date = d.spend_date
         WHERE  d.user_id IS NULL
         UNION
         SELECT d.*
         FROM   (SELECT *
                 FROM   spending
                 WHERE  platform = 'mobile') m
                RIGHT JOIN (SELECT *
                            FROM   spending
                            WHERE  platform = 'desktop') d
                        ON m.user_id = d.user_id
                           AND m.spend_date = d.spend_date
         WHERE  m.user_id IS NULL),
     both_cte
     AS (SELECT s.user_id,
                s.spend_date,
                'both' AS platform,
                s.amount
         FROM   (SELECT *
                 FROM   spending
                 UNION
                 SELECT DISTINCT NULL   AS user_id,
                                 spend_date,
                                 'both' AS platform,
                                 0      AS amount
                 FROM   spending) s
                LEFT JOIN all_cte c
                       ON s.user_id = c.user_id
                          AND s.spend_date = c.spend_date
                          
         WHERE  c.user_id IS NULL),
     union_cte
     AS (SELECT *
         FROM   both_cte
         UNION ALL
         SELECT *
         FROM   all_cte)
SELECT spend_date,
       platform,
       Sum(amount)             AS total_amount,
       Count(DISTINCT user_id) AS no_users
FROM   union_cte
GROUP  BY spend_date,
          platform
ORDER  BY spend_date,
          platform DESC 

-- COMMAND ----------

-- DBTITLE 1,Actual Solution
SELECT   spend_date,
         platform,
         Sum(amount)             AS total_amount,
         Count(DISTINCT user_id) AS no_users
FROM     (
                  SELECT   user_id,
                           spend_date,
                           Max(platform) AS platform,
                           Sum(amount)   AS amount
                  FROM     spending
                  GROUP BY user_id,
                           spend_date
                  HAVING   Count(DISTINCT platform) =1
                  UNION
                  SELECT   user_id,
                           spend_date,
                           'both'      AS platform,
                           Sum(amount) AS amount
                  FROM     spending
                  GROUP BY user_id,
                           spend_date
                  HAVING   Count(DISTINCT platform) =2
                  UNION
                  SELECT DISTINCT NULL AS user_id,
                                  spend_date,
                                  'both' AS platform,
                                  0      AS amount
                  FROM            spending) a
GROUP BY spend_date,
         platform
ORDER BY spend_date,
         platform DESC
