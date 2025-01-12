# Shell_project
Order of events:

1. **landing zone**
   -- Contains raw data from th source and the filename is appended as a new column like     
      "hdfs://m01.itversity.com:9000/user/itv015202/Project/NMIS3.csv" 
2. **staging zone**
   -- Based on the analysis there were anamolies like missing aesttime, quantity
   -- Entire row Duplicates in files NMIR2
   -- Zero/negative values if any
   -- unit may be in kwh/mwh/wh and transformation applied
   cast(quantity as decimal)
    when lower(unit) = 'mwh' then 1000* cast(quantity as decimal)
    when lower(unit) = 'wh' then cast(quantity as decimal)/1000 
    else  0.00 
   
4. **content zone**
   -- Designed 
   
6. fact_dimension_table
7. operating_hours_final_output
