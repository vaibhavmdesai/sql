-- Databricks notebook source
drop table billings

-- COMMAND ----------

create table billings 
(
emp_name varchar(10),
bill_date date,
bill_rate int
);
delete from billings;
insert into billings values
('Sachin','1990-01-01',25)
,('Sehwag' ,'1989-01-01', 15)
,('Dhoni' ,'1989-01-01', 20)
,('Sachin' ,'1991-02-05', 30)
;

create table HoursWorked 
(
emp_name varchar(20),
work_date date,
bill_hrs int
);
insert into HoursWorked values
('Sachin', '1990-07-01' ,3)
,('Sachin', '1990-08-01', 5)
,('Sehwag','1990-07-01', 2)
,('Sachin','1991-07-01', 4)

-- COMMAND ----------

select * from billings

-- COMMAND ----------

select * from HoursWorked

-- COMMAND ----------

with cte
as
(
select *, lead(date_add(bill_date, -1), 1, '9999-12-31') over(partition by emp_name order by bill_date asc) as bill_end_date
from billings
)
select hw.emp_name, sum(hw.bill_hrs * cte.bill_rate) as total_sum
from cte join HoursWorked hw on cte.emp_name = hw.emp_name
and hw.work_date between cte.bill_date and cte.bill_end_date
group by hw.emp_name
