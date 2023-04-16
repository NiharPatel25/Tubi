/*The duration information is present in the session_table. 
There is no information about the minimum time needed between 2 sessions to consider them separate sessions. 
However, that is not needed for the analysis here. 
We will also focus on only the start date of a session and not consider the end date based on the duration. 
So if a consumer starts a session on Jan 1st 11:59 pm and the duration is 60 mins changing the date to Jan 2nd, 
we will consider the session belongs to Jan  1st irrespective of the end time. 
*/

---The code is mainly written with postgresql in mind. But should work for most types of SQL services. 


-----QUESTION 1 : List top 5 device_ids by total duration in September 2019. Exclude device_ids with country Canada (CA).
--This should help us identify our highest engaging customers. Understanding their behavior can help us optimize for other customers as well. 

--solution 1
Select 
	a.device_id, 
	sum(a.duration) total_duration, 
from session_table as a 
Join user_table as b on a.device_id = b.device_id
where year(a.session_start) = 2019 and month(a.session_start) = 9 and b.country <> ‘CA’
group by a.device_id
order by 2
limit 5

--solution 2
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

-----QUESTION 2. How many devices watched at least 10 unique days in the time period August 1, 2019 to September 1, 2019 
--and they watched more than 10 hours in that same time period? */
---APPLICATION: Isolating most engaging customers and learning their behavior


with agg as 
(Select 
	device_id, 
	count(distinct substr(cast(session_start as varchar(64)), 1,10)) distinct_dates, 
        ---Removing the time stamps from datetime because we only need date. will need to use substring for mysql
	cast(sum(duration) as float)/3600 as duration_hours ---converting to hours since its in seconds
from session_table as a 
where session_start between cast('2019-08-01' as date) and cast('2019-09-01' as date)
group by 1
)
select 
    count(distinct device_id) total_devices
from agg 
where duration_hours > 10 and distinct_dates >= 10
;

-----QUESTION 3: 
/* What is the average total time viewed on each day of the week per platform 
 * for the time period August 1, 2019 to September 1, 2019. 
 * The results should have one row per day of the week (Sunday, Monday, Tuesday, etc.) and should be expressed in hours. 
 */ 
---APPLICATION: 
---this should help us identify number days with highest engagement and platforms with highest engagement. 
---Can be used to identify when to run experiments or marketing campaigns

----Solution 1
---we have calculated average per day per platform by dividing the total duration by concatenated field of date and platform
select 
      To_Char("session_start", 'DAY') as week_day, 
      cast(sum(duration) as float)/(3600*count(distinct concat(substr(cast(session_start as varchar(64)), 1,10), b.platform))
          as total_duration_per_day_per_platform
from session_table as a 
Join user_table as b on a.device_id = b.device_id
group by 1

---Here I am giving a break down with every day but also by platform. So we can see what is the average viewership by every platform per day. 
----Solution 2
select 
   b.platform, 
   To_Char("session_start", 'DAY') as week_day, 
   cast(sum(duration) as float)/(3600*count(distinct substr(cast(session_start as varchar(64)), 1,10))) total_duration_per_day
from session_table as a 
Join user_table as b on a.device_id = b.device_id
group by 1,2
order by 1,2
				    
---If we dont have postgres sql we can use something like this to get the days: 
---Trunc of date gives the date at start of week by default its monday. Subtracting start of week with other days will tell us which day it is. 
--- case when DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 0 then 'Monday' 
---when DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 1 then 'Tuesday'
---when DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 2 then 'Wedsday'
---when DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 3 then 'Thursday'
---when DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 4 then 'Friday'
---when DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 5 then 'Saturday'
---when DATEDIFF(session_start, DATE_TRUNC('week', cast(session_start as date))) = 6 then 'Sunday'end 

--------QUESTION 4: List top 5 users with the highest total duration per country for each month in 2019.

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
   m.*, 
   row_number()over(partition by country, month_session order by total_duration desc) row_value -----if 2 users are having same rank we are not considering it here
from 
   main as m)
select 
    month_session, 
    country, 
    device_id,
    total_duration
from sorted
where row_value <= 5
;

-------QUESTION 5: 
/* List the device_id and the country of active users. Active users are those who logged in
to their accounts for 5 or more consecutive days. Return the result table ordered by the
device_id.*/
---APPLICATION: Helps us identify users who engage more frequently, and what kind of shows do these users interact with. 
---These might not necessarily be users who have highest duration. 

---Only considering consecutive days based on start date. 
---If a user has a start datetime at 11:59 PM and the duration extends the view time to another day, we will not consder it to be activity on both days

with consecutive as 
(SELECT 
   device_id, 
   session_start,
   row_number() over( partition by device_id order by session_start) rank_cat
FROM session_table
),
active_users as 
(select 
   device_id
   session_start - rank_cat consecutive_group,---for a particular group of consecutive days, what are the number of days in them
   count(distinct session_start) filter_for_days
from consecutive
group by 1,2)
select 
    a.device_id, 
    b.country
from (select distinct device_id from active_users where filter_for_days >= 5) as a 
left join user_table as b on a.device_id = b.device_id
order by a.device_id




