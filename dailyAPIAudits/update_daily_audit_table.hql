SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;

INSERT OVERWRITE TABLE daily_audit_data
PARTITION(date_of_pull)
SELECT 
source_name,
COUNT(source_name) AS count_downloaded,
FROM_UNIXTIME(timestamp,'yyyy-MM-dd') as date_of_pull
FROM 
cdr 
WHERE FROM_UNIXTIME(timestamp,'yyyy-MM-dd')=FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')
OR FROM_UNIXTIME(timestamp,'yyyy-MM-dd')=DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),1)
GROUP BY source_name,FROM_UNIXTIME(timestamp,'yyyy-MM-dd');