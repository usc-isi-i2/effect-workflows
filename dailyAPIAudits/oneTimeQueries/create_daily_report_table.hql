CREATE TABLE IF NOT EXISTS daily_audit_report (
source_name string,
count_downloaded INT,
weekly_avg INT,
last_date_of_pull STRING,
average INT,
median INT)
PARTITIONED BY (date_of_pull STRING)
ROW FORMAT delimited
fields terminated by ','
lines terminated by '\n'
STORED AS textfile;
