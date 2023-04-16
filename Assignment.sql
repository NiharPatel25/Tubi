--QUESTION 1 : List top 5 device_ids by total duration in September 2019. Exclude device_ids with country Canada (CA).

---solution 1
Select 
	a.device_id, 
	sum(a.duration) total_duration, 
from session_table as a 
Join user_table as b on a.device_id = b.device_id
where year(a.session_start) = 2019 and month(a.session_start) = 9 and b.country <> ‘CA’
group by a.device_id
order by 2
limit 5

---solution 2
with agg as 
(Select 
	a.device_id, 
	sum(a.duration) total_duration 
from session_table as a 
Join user_table as b on a.device_id = b.device_id
where year(a.session_start) = 2019 and month(a.session_start) = 9 and b.country <> ‘CA’
group by a.device_id)
select 
    device_id, 
    total_duration
from 
(select 
   device_id, 
   total_duration, 
   ROW_NUMBER () over (order by total_duration desc) filter_top_5
from agg) as fin 
where filter_top_5 <= 5

--QUESTION 2. How many devices watched at least 10 unique days in the time period August 1, 2019 to September 1, 2019 
--and they watched more than 10 hours in that same time period? */

with agg as 
(Select 
	device_id, 
	count(distinct substr(cast(session_start as varchar(64)), 1,10)) distinct_dates, ---will need to use substring for mysql
	cast(sum(duration) as float)/3600 duration_hours ---converting to hours since its in seconds
from session_table as a 
where session_start between cast('2019-08-01' as date) and cast('2019-09-01' as date)
group by 1
)
select 
    count(distinct device_id) total_devices
from agg 
where duration_hours > 10 and distinct_dates >= 10
;

----QUESTION 3: 
/* What is the average total time viewed on each day of the week per platform 
 * for the time period August 1, 2019 to September 1, 2019. 
 * The results should have one row per day of the week (Sunday, Monday, Tuesday, etc.) and should be expressed in hours. 
 */ 


select 
   b.platform, 
   case when 
      DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 0 then 'Monday' 
      ---Trunc of date gives the date at start of week by default its monday. Subtracting start of week with other days will tell us which day it is. 
      --we can use To_Char("timestamp", 'DAY') in postgres 
      when 
      DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 1 then 'Tuesday'
      when 
      DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 2 then 'Wedsday'
      when 
      DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 3 then 'Thursday'
      when 
      DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 4 then 'Friday'
      when 
      DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 5 then 'Saturday'
      when 
      DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 6 then 'Sunday'
      end as week_day, 
      cast(sum(duration) as float)/(3600*count(distinct substring(cast(session_start as varchar(64)), 1,10))) total_duration_per_day
from session_table as a 
Join user_table as b on a.device_id = b.device_id
group by 1,2

------List top 5 users with the highest total duration per country for each month in 2019.
with main as 
(select 
   month(a.session_date) month_session, 
   b.country, 
   device_id,
   sum(a.duration) total_duration
from session_table as a 
Join user_table as b on a.device_id = b.device_id
where year(a.session_start) = 2019
group by 1,2,3), 
sorted as 
(select 
   main.*, 
   row_number()over(partition by country, month_session order by total_duration desc) row_value
from 
   main)
select 
    month_session, 
    country, 
    total_duration
from sorted
where row_value <= 5
;


/*5. List the device_id and the country of active users. Active users are those who logged in
to their accounts for 5 or more consecutive days. Return the result table ordered by the
device_id.*/

---Only considering consecutive days based on start date. If a user has a start datetime at 11:59 PM and the duration extends the view time to another day, we will not consder it to be  activity on both days

with consecutive as 
(SELECT 
   device_id, 
   session_start,
   dateadd( d, -row_number() over( partition by device_id order by session_start), session_start) consecutive_group
---this can be done without the dateadd even if date is converted to just integer
FROM session_table
),
active_users as 
(select 
   device_id
   consecutive_group,
   count(distinct session_start) filter_for_days
from consecutive
group by 1,2
having filter_for_days > 5)
select 
    a.device_id, 
    b.country
from (select distinct device_id from active_users) as a 
left join user_table as b on a.device_id = b.device_id
order by a.device_id




