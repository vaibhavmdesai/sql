-- Databricks notebook source
create table tasks (
date_value date,
state varchar(10)
);

insert into tasks  values ('2019-01-01','success'),('2019-01-02','success'),('2019-01-03','success'),('2019-01-04','fail')
,('2019-01-05','fail'),('2019-01-06','success')

-- COMMAND ----------

SELECT
  MIN(date_value) AS start_date,
  MAX(date_value) AS end_date,
  state
FROM
  (
    SELECT
      *,
      date_add(
        date_value,
        -1 * row_number() over(
          partition by state
          ORDER BY
            date_value
        )
      ) AS rn
    FROM
      tasks
  ) a
GROUP BY
  a.rn,
  a.state
ORDER BY
  start_date
