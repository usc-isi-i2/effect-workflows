SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;

INSERT INTO TABLE daily_audit_data
SELECT 
FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') as date_of_pull,
source_name,
count(source_name) 
FROM 
cdr 
WHERE FROM_UNIXTIME(timestamp,'yyyy-MM-dd')=FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')
GROUP BY source_name;