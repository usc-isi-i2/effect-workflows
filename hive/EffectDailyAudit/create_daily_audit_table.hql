--create daily audit table

ALTER TABLE daily_audit_data RENAME TO daily_audit_data_backup;

CREATE TABLE IF NOT EXISTS daily_audit_data (
date_of_pull string,
source_name string,
count INT)
ROW FORMAT delimited
fields terminated by ','
lines terminated by '\n'
STORED AS textfile;
