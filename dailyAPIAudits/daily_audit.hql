--update the daily audit table

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;

INSERT INTO TABLE daily_audit_data
SELECT
FROM_UNIXTIME(TIMESTAMP,"yyyy-MM-dd") AS date_of_pull,
source_name,
COUNT(*) AS count_downloaded
FROM cdr
GROUP BY
FROM_UNIXTIME(TIMESTAMP,"yyyy-MM-dd"),source_name;
