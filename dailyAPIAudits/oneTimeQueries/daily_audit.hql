--update the daily audit table

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;
SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;

INSERT OVERWRITE TABLE daily_audit_data
PARTITION(date_of_pull)
SELECT
source_name,
COUNT(*) AS count_downloaded,
FROM_UNIXTIME(TIMESTAMP,"yyyy-MM-dd") AS date_of_pull
FROM cdr
WHERE FROM_UNIXTIME(TIMESTAMP,"yyyy-MM-dd")<FROM_UNIXTIME(UNIX_TIMESTAMP(),"yyyy-MM-dd")
GROUP BY
FROM_UNIXTIME(TIMESTAMP,"yyyy-MM-dd"),source_name;
