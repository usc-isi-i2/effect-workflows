--create daily audit table

CREATE TABLE IF NOT EXISTS daily_audit_data (
source_name string,
count_downloaded INT)
PARTITIONED BY (date_of_pull STRING)
ROW FORMAT delimited
fields terminated by ','
lines terminated by '\n'
STORED AS textfile;

