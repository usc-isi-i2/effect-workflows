SET hive.exec.parallel=true;
SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=false;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
// set hive.exec.max.dynamic.partitions=1000;
// set hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE daily_audit_report 
PARTITION(date_of_pull)
SELECT
CASE WHEN (a.source_name IS NOT NULL) THEN a.source_name ELSE b.source_name END,
CASE WHEN (c.count_downloaded IS NULL) THEN 0 ELSE c.count_downloaded END,
CASE WHEN (b.weekly_avg IS NULL) THEN 0 ELSE b.weekly_avg END,
d.last_date_of_pull,
CAST(a.average AS INT),
CAST(a.median AS INT),
FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') as date_of_pull
FROM
(SELECT 
source_name,
AVG(count_downloaded) as average,
PERCENTILE(count_downloaded,0.5) as median FROM
daily_audit_data 
GROUP BY source_name) a
FULL OUTER JOIN
(SELECT 
source_name,
AVG(count_downloaded) AS weekly_avg 
FROM daily_audit_data WHERE WEEKOFYEAR(date_of_pull)=WEEKOFYEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'))-1
AND YEAR(date_of_pull)=YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'))
GROUP BY YEAR(date_of_pull),WEEKOFYEAR(date_of_pull),source_name) b
ON a.source_name=b.source_name
FULL OUTER JOIN
(SELECT
source_name,
count(source_name) as count_downloaded
FROM
cdr
WHERE FROM_UNIXTIME(timestamp,"yyyy-MM-dd")=FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')
GROUP BY source_name) c
ON
a.source_name=c.source_name
FULL OUTER JOIN
(SELECT
source_name,
max(date_of_pull) AS last_date_of_pull
FROM daily_audit_data where count_downloaded>0
GROUP BY source_name) d
ON a.source_name=d.source_name;