-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### https://leetcode.com/problems/trips-and-users/
-- MAGIC
-- MAGIC The cancellation rate is computed by dividing the number of canceled (by client or driver) requests with unbanned users by the total number of requests with unbanned users on that day.
-- MAGIC Write a SQL query to find the cancellation rate of requests with unbanned users (both client and driver must not be banned) each day between "2013-10-01" and "2013-10-03". Round Cancellation Rate to two decimal points.
-- MAGIC Return the result table in any order.
-- MAGIC The query result format is in the following example.

-- COMMAND ----------

Create table  Trips (id int, client_id int, driver_id int, city_id int, status varchar(50), request_at varchar(50));
Create table Users (users_id int, banned varchar(50), role varchar(50));
Truncate table Trips;
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('1', '1', '10', '1', 'completed', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('2', '2', '11', '1', 'cancelled_by_driver', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('3', '3', '12', '6', 'completed', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('4', '4', '13', '6', 'cancelled_by_client', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('5', '1', '10', '1', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('6', '2', '11', '6', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('7', '3', '12', '6', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('8', '2', '12', '12', 'completed', '2013-10-03');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('9', '3', '10', '12', 'completed', '2013-10-03');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('10', '4', '13', '12', 'cancelled_by_driver', '2013-10-03');
Truncate table Users;
insert into Users (users_id, banned, role) values ('1', 'No', 'client');
insert into Users (users_id, banned, role) values ('2', 'Yes', 'client');
insert into Users (users_id, banned, role) values ('3', 'No', 'client');
insert into Users (users_id, banned, role) values ('4', 'No', 'client');
insert into Users (users_id, banned, role) values ('10', 'No', 'driver');
insert into Users (users_id, banned, role) values ('11', 'No', 'driver');
insert into Users (users_id, banned, role) values ('12', 'No', 'driver');
insert into Users (users_id, banned, role) values ('13', 'No', 'driver');

-- COMMAND ----------

select * from Users

-- COMMAND ----------

select * from Trips

-- COMMAND ----------

-- Step1 : the total number of requests with unbanned users on that day
WITH 
unbanned_clients AS (
  SELECT
    t.request_at,
    t.id
  FROM
    trips t
    INNER JOIN users u ON t.client_id = u.users_id
  WHERE
    u.banned = 'No'
)
,unbanned_drivers AS (
  SELECT
    t.request_at,
    t.id
  FROM
    trips t
    INNER JOIN users u ON t.driver_id = u.users_id
  WHERE
    u.banned = 'No'
)
,final_cte (
  SELECT
    ud.request_at,
    count(1) AS unbanned_trips,
    sum(
      CASE
        WHEN t.status <> 'completed' THEN 1
        ELSE 0
      END
    ) cancelled_trips
  FROM
    unbanned_drivers ud
    JOIN unbanned_clients uc ON ud.id = uc.id
    JOIN trips t ON t.id = ud.id
  GROUP BY
    ud.request_at
)
SELECT
  request_at,
  round(cancelled_trips * 1.0 / unbanned_trips, 2) AS cancellation_rate
FROM
  final_cte
ORDER BY
  request_at

-- COMMAND ----------

WITH unbanned_clients AS (
  SELECT
    *
  FROM
    trips t
    INNER JOIN users u ON t.client_id = u.users_id
    INNER JOIN users d ON t.driver_id = d.users_id
  WHERE
    u.banned = 'No'
    AND d.banned = 'No'
)
SELECT
  request_at,
  ROUND(
    (
      SUM(
        CASE
          WHEN status <> 'completed' THEN 1
          ELSE 0
        END
      ) * 1.0
    ) / COUNT(1),
    2
  ) as cancelled_percentage
FROM
  unbanned_clients
GROUP BY
  request_at
ORDER BY request_at
