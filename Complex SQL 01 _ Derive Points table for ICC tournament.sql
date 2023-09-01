-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #### https://www.youtube.com/watch?v=qyAgWL066Vo&list=PLBTZqjSKn0IeKBQDjLmzisazhqQy4iGkb

-- COMMAND ----------

-- DBTITLE 1,Without Draws
CREATE TABLE icc_world_cup
(
team_1 varchar(20),
team_2 varchar(20),
winner varchar(20)
);

INSERT INTO icc_world_cup VALUES('India','SL','India');
INSERT INTO icc_world_cup VALUES('SL','Aus','Aus');
INSERT INTO icc_world_cup VALUES('SA','Eng','Eng');
INSERT INTO icc_world_cup VALUES('Eng','NZ','NZ');
INSERT INTO icc_world_cup VALUES('Aus','India','India');

-- COMMAND ----------

select * from icc_world_cup

-- COMMAND ----------

-- DBTITLE 1,My Solution
WITH union_cte
AS (SELECT
  team_1 AS team
FROM icc_world_cup
UNION ALL
SELECT
  team_2 AS team
FROM icc_world_cup),
matches_played_cte
AS (SELECT
  team,
  COUNT(1) AS matches_played
FROM union_cte
GROUP BY team)
SELECT
  mp.team,
  mp.matches_played,
  COUNT(wc.winner) AS no_of_wins,
  mp.matches_played - COUNT(wc.winner) AS no_of_losses
FROM matches_played_cte mp
LEFT JOIN icc_world_cup wc
  ON mp.team = wc.winner
GROUP BY mp.team,
         mp.matches_played

-- COMMAND ----------

-- DBTITLE 1,Actual Solution
with cte as
(
select team_1 as team, case when team_1==winner then 1 else 0 end as win_flag, case when team_1 <> winner then 1 else 0 end as loss_flag from icc_world_cup
union all
select team_2 as team, case when team_2==winner then 1 else 0 end as win_flag, case when team_2 <> winner then 1 else 0 end as loss_flag from icc_world_cup
)
select team, count(1) as matches_played, sum(win_flag) as no_of_wins, sum(loss_flag) as no_of_losses
from cte
group by team

-- COMMAND ----------

-- DBTITLE 1,With draws
CREATE TABLE icc_world_cup_draws
(
team_1 varchar(20),
team_2 varchar(20),
winner varchar(20)
);

INSERT INTO icc_world_cup_draws VALUES('India','SL','India');
INSERT INTO icc_world_cup_draws VALUES('SL','Aus','Aus');
INSERT INTO icc_world_cup_draws VALUES('SA','Eng','Eng');
INSERT INTO icc_world_cup_draws VALUES('Eng','NZ','NZ');
INSERT INTO icc_world_cup_draws VALUES('Aus','India','India');
INSERT INTO icc_world_cup_draws VALUES('WI','India','Draw');

-- COMMAND ----------

select * from icc_world_cup_draws
