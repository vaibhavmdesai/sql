-- Databricks notebook source
CREATE table activity
(
user_id varchar(20),
event_name varchar(20),
event_date date,
country varchar(20)
);
delete from activity;
insert into activity values (1,'app-installed','2022-01-01','India')
,(1,'app-purchase','2022-01-02','India')
,(2,'app-installed','2022-01-01','USA')
,(3,'app-installed','2022-01-01','USA')
,(3,'app-purchase','2022-01-03','USA')
,(4,'app-installed','2022-01-03','India')
,(4,'app-purchase','2022-01-03','India')
,(5,'app-installed','2022-01-03','SL')
,(5,'app-purchase','2022-01-03','SL')
,(6,'app-installed','2022-01-04','Pakistan')
,(6,'app-purchase','2022-01-04','Pakistan');

-- COMMAND ----------

select * from activity

-- COMMAND ----------

-- DBTITLE 1,Active users per day
select event_date, count(distinct user_id) as active_users
from activity
group by event_date
order by event_date

-- COMMAND ----------

-- DBTITLE 1,active users by week
select extract(week from event_date), count(distinct user_id) as active_users
from activity
group by extract(week from event_date)
order by extract(week from event_date)

-- COMMAND ----------

-- DBTITLE 1,datewise total users who installed and purchased the app on same day
WITH install_cte AS (
    SELECT *
    FROM activity
    WHERE event_name = 'app-installed'
),
purchase_cte AS (
    SELECT *
    FROM activity
    WHERE event_name = 'app-purchase'
)
SELECT COALESCE(i.event_date, p.event_date) AS event_date1,
    SUM(
        CASE
            WHEN i.user_id IS NOT NULL
            AND p.user_id IS NOT NULL THEN 1
            ELSE 0
        END
    ) AS flg
FROM install_cte i
    FULL OUTER JOIN purchase_cte p ON i.user_id = p.user_id
    AND i.event_date = p.event_date
GROUP BY event_date1
ORDER BY event_date1

-- COMMAND ----------

-- DBTITLE 1,% of Paid users in India, USA...any other country should be tagged as others
SELECT DISTINCT CASE
        WHEN country IN ('India', 'USA') THEN country
        ELSE 'Others'
    END AS country_group,
    COUNT(1) OVER () AS total_count,
    COUNT(1) OVER (
        PARTITION BY CASE
            WHEN country IN ('India', 'USA') THEN country
            ELSE 'Others'
        END
        ORDER BY 1
    ) country_count,
    (
        COUNT(1) OVER (
            PARTITION BY CASE
                WHEN country IN ('India', 'USA') THEN country
                ELSE 'Others'
            END
            ORDER BY 1
        ) / COUNT(1) OVER ()
    ) * 100 AS paid_per
FROM activity
WHERE event_name = 'app-purchase'

-- COMMAND ----------

-- DBTITLE 1,datewise total users who installed and purchased the app on very next day
WITH install_cte AS (
    SELECT *
    FROM activity
    WHERE event_name = 'app-installed'
),
purchase_cte AS (
    SELECT *
    FROM activity
    WHERE event_name = 'app-purchase'
)
SELECT COALESCE(p.event_date, i.event_date) AS event_date1,
    SUM(
        CASE
            WHEN i.user_id IS NOT NULL
            AND p.user_id IS NOT NULL THEN 1
            ELSE 0
        END
    ) AS flg
FROM install_cte i
    FULL JOIN purchase_cte p ON i.user_id = p.user_id
    AND i.event_date = date_add(p.event_date, -1)
GROUP BY event_date1
ORDER BY event_date1
