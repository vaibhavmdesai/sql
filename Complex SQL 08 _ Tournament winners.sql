-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC The winner in each group is the player who scored the maximum total points within the group. In the case of a tie, the lowest player_id wins.
-- MAGIC
-- MAGIC Write an SQL query to find the winner in each group.
-- MAGIC
-- MAGIC

-- COMMAND ----------

create table players
(player_id int,
group_id int);

insert into players values (15,1);
insert into players values (25,1);
insert into players values (30,1);
insert into players values (45,1);
insert into players values (10,2);
insert into players values (35,2);
insert into players values (50,2);
insert into players values (20,3);
insert into players values (40,3);

create table matches
(
match_id int,
first_player int,
second_player int,
first_score int,
second_score int);

insert into matches values (1,15,45,3,0);
insert into matches values (2,30,25,1,2);
insert into matches values (3,30,15,2,0);
insert into matches values (4,40,20,5,2);
insert into matches values (5,35,50,1,1);

-- COMMAND ----------

select * from players
where player_id = 30

-- COMMAND ----------

select * from matches

-- COMMAND ----------

WITH CTE
     AS (SELECT p.player_id
                ,p.group_id
                ,m1.first_score AS score
         FROM   players p
                INNER JOIN matches m1
                        ON p.player_id = m1.first_player
         UNION ALL
         SELECT p.player_id
                ,p.group_id
                ,m2.second_score AS score
         FROM   players p
                INNER JOIN matches m2
                        ON p.player_id = m2.second_player),
     SCORE_CTE
     AS (SELECT player_id
                ,group_id
                ,ROW_NUMBER()
                   OVER(
                     PARTITION BY group_id
                     ORDER BY Sum(score) DESC, player_id ASC ) AS rn
         FROM   CTE
         GROUP  BY player_id
                   ,group_id)
SELECT group_id
       ,player_id
FROM   SCORE_CTE
WHERE  rn = 1
ORDER  BY 1 

-- COMMAND ----------

WITH CTE
     AS (SELECT first_player as player_id
                ,first_score AS score
         FROM   matches
         UNION ALL
         SELECT second_player as player_id
                ,second_score AS score
        FROM matches
        ),
     SCORE_CTE
     AS (SELECT player_id
                ,group_id
                ,ROW_NUMBER()
                   OVER(
                     PARTITION BY group_id
                     ORDER BY Sum(score) DESC, player_id ASC ) AS rn
         FROM   CTE
         GROUP  BY player_id
                   ,group_id)
SELECT group_id
       ,player_id
FROM   SCORE_CTE
WHERE  rn = 1
ORDER  BY 1 
