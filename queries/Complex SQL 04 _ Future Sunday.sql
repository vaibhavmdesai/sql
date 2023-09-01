-- Databricks notebook source
-- MAGIC %py
-- MAGIC
-- MAGIC from pyspark.sql.functions import explode, sequence, to_date
-- MAGIC
-- MAGIC beginDate = '2022-01-01'
-- MAGIC endDate = '2050-12-31'
-- MAGIC
-- MAGIC (
-- MAGIC   spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
-- MAGIC     .createOrReplaceTempView('dates')
-- MAGIC )
-- MAGIC

-- COMMAND ----------

select calendarDate, row_number() over(order by calendarDate) as rn from dates
where date_format(calendarDate, 'EEEE') = 'Sunday'
