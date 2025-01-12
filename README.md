# Shell_project
Order of events:

1. **landing zone**
   -- Contains raw data from th source and the filename is appended as a new column like     
      "hdfs://m01.itversity.com:9000/user/itv015202/Project/NMIS3.csv" 
2. **staging zone**
   -- Based on the analysis there were anamolies like missing aesttime, quantity (**filtered not null values**)
   -- Entire row Duplicates in files NMIR2 (**added distinct on entire row**)
   -- Zero/negative values if any (**applied filter with unit > 1.0)**
   -- unit may be in kwh/mwh/wh and below transformation applied to convert it to a unified **kwh** unit
   ** cast(quantity as decimal)
    when lower(unit) = 'mwh' then 1000* cast(quantity as decimal)
    when lower(unit) = 'wh' then cast(quantity as decimal)/1000 
    else  0.00 end as qty **
   
4. **content zone**
   -- Designed to hold date and time hours for each nmi so that it can be used for further analysis.
   select  a.date
   ,datekey
   ,aesttime
   ,qty
   ,unit
   ,lower(a.nmi) as nmi
   ,substring(aesttime::time::varchar(20), 1,5) as time_hours
   from sz_nmis3 a inner join
   nmi_info n on lower(a.nmi) = lower(n.nmi) 
   inner join dim_time on substring(time::varchar(20), 1,5) = substring(a.aesttime::time::varchar(20), 1,5)
   inner join Dim_Date e on e.Date = a.date
   where aesttime is not null;
   
5. **fact_dimension_table**
   -- Dim_date  --> Created the daily data from 2017-10-01 holds sample for few days
   -- Dim time  --> Created for 15 min interval for 24 hours
   -- dim_nmi_info  --> same as the source file
   -- fact_usage --> Created a fact table to hold data at the lowest granularity of time_hours in 15min or 30 min as per nmi_infor file
6. **operating_hours_final_output**
   --Below is the query 
   with a as (select  date , 
   aesttime,
   qty, 
   unit,
   lower(a.nmi) as nmi,
   round(cast(n.interval as numeric)/60,2) as interval_factor ** --Created a interval_factor to convert 15min and 30 min interval to decimals**
   from sz_nmig2 a inner join nmi_info n on lower(a.nmi) = lower(n.nmi) 
   where aesttime is not null 
   and qty > 1.0) **-- filtered out time slots which has unit lesser than 1 kwh**
   
   ,b as (
   select date, avg(qty) as avg_qty from a group by 1 order by 1) **compute avg for getting records which are more than the avg**
   
   ,operating_hours as 
   (select b.date, aesttime, qty, unit, avg_qty, day_of_weel, a.nmi, a.interval_factor
   from a inner join b  
   on a.date = b.date
   inner join Day_Table_1 d
   on d.as_of_Date = b.date
   and d.as_of_Date = a.date
   where a.qty > b.avg_qty and a.date > '2017-10-01') ** --compute avg for getting records which are more than the avg**
   
   ,c as (select *, lag(aesttime) over(partition by date order by date) 
   as prev_time from operating_hours) ** --get the previous row's time** 
   
   ,d as (select date,
   aesttime::timestamp start_time, prev_time::timestamp end_time
   ,round( cast(EXTRACT(EPOCH FROM (aesttime::timestamp - prev_time::timestamp))/3600 as numeric), 2) as diff_time ** --Get the time diff**
   , nmi, interval_factor from c) 
   
   ,e as (select nmi,extract(hour from start_time) start_t, count(*) count_of, length(count(*)::Varchar(10)) leng
   from d where diff_time is not null and diff_time =interval_factor group by 1, 2) ** --considering only the records with continous difference matching the interval**

   ,f as (select leng, count(*) cnt from e group by 1) ** --getting the count of records and their corresponding length**
   select nmi, min(start_t), max(start_t) from e where leng = 
   (select leng from f where cnt = (select max(cnt) from f)) group by 1;  ** --arrived at filtering out only the records with highly repeating records**
   7. Dimensional data model
   8. Bar chart for operating hours for each nmi
