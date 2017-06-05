--create daily audit table

CREATE TABLE IF NOT EXISTS daily_audit_data (
date_of_pull string,
source_name string,
count_downloaded INT)
ROW FORMAT delimited
fields terminated by ','
lines terminated by '\n'
STORED AS textfile;

