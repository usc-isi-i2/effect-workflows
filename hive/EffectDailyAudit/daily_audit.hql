--update the daily audit table

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;

INSERT INTO table daily_audit_data
SELECT
FROM_UNIXTIME(TIMESTAMP,"MM/dd/yyyy") AS date_of_pull,
source_name,
COUNT(*) AS count
FROM cdr
GROUP BY
FROM_UNIXTIME(TIMESTAMP,"MM/dd/yyyy"),source_name;

DROP TABLE IF EXISTS daily_audit_data_backup;
