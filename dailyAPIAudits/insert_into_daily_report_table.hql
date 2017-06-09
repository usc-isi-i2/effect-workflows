SET hive.exec.parallel=true;
SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=false;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;


INSERT OVERWRITE TABLE daily_audit_report 
PARTITION(date_of_pull)
SELECT
CASE WHEN (a.source_name IS NOT NULL) THEN a.source_name ELSE c.source_name END,
c.last_date_of_pull,
CASE WHEN (b.count_downloaded IS NULL) THEN 0 ELSE b.count_downloaded END,
CASE WHEN (d.count_downloaded IS NULL) THEN 0 ELSE d.count_downloaded END,
CASE WHEN (e.count_downloaded IS NULL) THEN 0 ELSE e.count_downloaded END,
CASE WHEN (f.count_downloaded IS NULL) THEN 0 ELSE f.count_downloaded END,
CASE WHEN (g.count_downloaded IS NULL) THEN 0 ELSE g.count_downloaded END,
CASE WHEN (h.count_downloaded IS NULL) THEN 0 ELSE h.count_downloaded END,
CASE WHEN (i.count_downloaded IS NULL) THEN 0 ELSE i.count_downloaded END,
CAST(a.average AS INT),
0,
FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') as date_of_pull
FROM
(SELECT source_name,
sum(count_downloaded)/datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),b.min_date) AS average
from daily_audit_data a JOIN 
(select min(date_of_pull) AS min_date, source_name from daily_audit_data group by source_name) b
on a.source_name=b.source_name 
GROUP BY a.source_name,b.min_date) a
FULL OUTER JOIN
(SELECT
source_name,
count(source_name) as count_downloaded
FROM
cdr
WHERE FROM_UNIXTIME(timestamp,"yyyy-MM-dd")=FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')
GROUP BY source_name) b
ON
a.source_name=b.source_name
FULL OUTER JOIN
(SELECT
source_name,
max(date_of_pull) AS last_date_of_pull
FROM daily_audit_data where count_downloaded>0
GROUP BY source_name) c 
ON a.source_name=c.source_name
FULL OUTER JOIN
(select source_name,count_downloaded,date_of_pull from daily_audit_data where date_of_pull=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)) d
on a.source_name=d.source_name
FULL OUTER JOIN
(select source_name,count_downloaded,date_of_pull from daily_audit_data where date_of_pull=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),2)) e
on a.source_name=e.source_name
FULL OUTER JOIN 
(select source_name,count_downloaded,date_of_pull from daily_audit_data where date_of_pull=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),3)) f
on a.source_name=f.source_name
FULL OUTER JOIN
(select source_name,count_downloaded,date_of_pull from daily_audit_data where date_of_pull=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),4)) g
on a.source_name=g.source_name
FULL OUTER JOIN
(select source_name,count_downloaded,date_of_pull from daily_audit_data where date_of_pull=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),5)) h
on a.source_name=h.source_name
FULL OUTER JOIN
(select source_name,count_downloaded,date_of_pull from daily_audit_data where date_of_pull=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),6)) i
on a.source_name=i.source_name;


