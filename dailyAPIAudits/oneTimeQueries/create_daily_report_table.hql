CREATE TABLE IF NOT EXISTS daily_audit_report (
source_name string,
last_date_of_pull STRING,
count_downloaded_today INT,
count_today_minus_1 INT,
count_today_minus_2 INT,
count_today_minus_3 INT,
count_today_minus_4 INT,
count_today_minus_5 INT,
count_today_minus_6 INT,
average INT,
median INT)
PARTITIONED BY (date_of_pull STRING)
ROW FORMAT delimited
fields terminated by ','
lines terminated by '\n'
STORED AS textfile;
