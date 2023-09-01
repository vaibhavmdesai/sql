-- Databricks notebook source
create table entries ( 
name varchar(20),
address varchar(20),
email varchar(20),
floor int,
resources varchar(10));

insert into entries 
values ('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR');

-- COMMAND ----------

select * from entries

-- COMMAND ----------

WITH visits
AS (SELECT name
         , COUNT(*)                               AS total_visits
         , concat_ws(',', collect_set(resources)) AS resources_used
    FROM entries
    GROUP BY name
   )
   , floor_visit
AS (SELECT name
         , floor
         , COUNT(*) AS number_of_visits
    FROM entries
    GROUP BY name
           , floor
   )
   , most_visited_floors
AS (SELECT name
         , floor
         , number_of_visits
         , ROW_NUMBER() OVER (PARTITION BY name ORDER BY number_of_visits DESC) rn
    FROM floor_visit
   )
SELECT v.name
     , v.total_visits
     , mvf.floor AS most_visited_floor
     , v.resources_used
FROM most_visited_floors mvf
    JOIN visits          v
        ON v.name = mvf.name
WHERE mvf.rn = 1
