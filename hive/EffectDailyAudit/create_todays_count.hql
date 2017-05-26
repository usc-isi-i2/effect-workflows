-- create a table for counts of records pulled from each api today's run
DROP TABLE IF EXISTS todays_count;


SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;

CREATE TABLE IF NOT EXISTS todays_count AS
SELECT
FROM_UNIXTIME(TIMESTAMP,'MM/dd/yyyy') AS date_of_pull,
source_name,
COUNT(*) AS count
FROM cdr
WHERE FROM_UNIXTIME(TIMESTAMP,'MM/dd/yyyy')=FROM_UNIXTIME(UNIX_TIMESTAMP(),'MM/dd/yyyy')
GROUP BY
FROM_UNIXTIME(TIMESTAMP,'MM/dd/yyyy'),source_name;
