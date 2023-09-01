-- Databricks notebook source
-- MAGIC %py
-- MAGIC friend_arr = [(1,2),
-- MAGIC (1,3),
-- MAGIC (2,1),
-- MAGIC (2,3),
-- MAGIC (3,5),
-- MAGIC (4,2),
-- MAGIC (4,3),
-- MAGIC (4,5)]
-- MAGIC
-- MAGIC df = spark.sparkContext.parallelize(friend_arr).toDF()
-- MAGIC friend_df = df.toDF('person_id', 'friend_id')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC person_arr = [(1,'Alice','alice2018@hotmail.com',88),
-- MAGIC (2,'Bob','bob2018@hotmail.com',11),
-- MAGIC (3,'Davis','davis2018@hotmail.com',27),
-- MAGIC (4,'Tara','tara2018@hotmail.com',45),
-- MAGIC (5,'John','john2018@hotmail.com',63)]
-- MAGIC
-- MAGIC df = spark.sparkContext.parallelize(person_arr).toDF()
-- MAGIC person_df = df.toDF('person_id', 'name', 'email', 'score')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC friend_df.createOrReplaceTempView('friend')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC person_df.createOrReplaceTempView('person')

-- COMMAND ----------

WITH friend_scores (
  SELECT
    f.person_id,
    f.friend_id,
    p.score AS friend_score
  FROM
    friend f
    INNER JOIN person p ON f.friend_id = p.person_id
)
SELECT
  p.person_id,
  p.name,
  count(*) AS total_friends,
  sum(friend_score) AS friends_score
FROM
  friend_scores fs
  INNER JOIN person p ON p.person_id = fs.person_id
GROUP BY
  p.person_id,
  p.name
HAVING
  sum(friend_score) > 100

