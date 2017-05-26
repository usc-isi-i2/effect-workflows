-- create a table with probable inconsistencies
DROP TABLE IF EXISTS daily_audit_irregularities;

CREATE TABLE IF NOT EXISTS daily_audit_irregularities  (
source_name string,
count INT,
threshold string)
ROW FORMAT delimited
fields terminated by '\t'
lines terminated by '\n'
STORED AS textfile
LOCATION '/user/hive/warehouse/effect_daily_irregularitites';
