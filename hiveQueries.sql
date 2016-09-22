# Create table CDR
CREATE TABLE CDR(`_id` STRING, timestamp INT, raw_content STRING, content_type STRING, url STRING, version STRING, team STRING, source_name STRING)
COMMENT 'Used to store all CDR data'
PARTITIONED BY (year INT, month INT, day INT)
STORED AS SEQUENCEFILE;


# Create table for hackmagadden
CREATE TABLE hackmageddon (raw_content STRING)
STORED AS TEXTFILE

# Load data into hackmagadden
LOAD DATA INPATH '/user/effect/hackmageddon_20160730.jl' INTO TABLE hackmageddon


# Load data into CDR from hackmagadden
FROM hackmageddon h
INSERT OVERWRITE TABLE cdr PARTITION(year='2016', month='09', day='22')
SELECT concat('hackmageddon/', hex(hash(h.raw_content))), unix_timestamp(), 
		h.raw_content, 'application/json', 
		concat('http://effect.isi.edu/input/hackmageddon/',hex(hash(h.raw_content))), 
		"2.0", "ISI", "hackmageddon"



#---------------------------------------------------------------------------------------

# Create table CDR Staging and load data from that into CDR
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
FROM
    cdr_staging
INSERT OVERWRITE TABLE
    cdr
PARTITION(year, month, day)
SELECT
    `_id`, timestamp, raw_content, content_type, url, version, team, year(from_unixtime(timestamp)) AS year, month(from_unixtime(timestamp)) AS month, day(from_unixtime(timestamp)) AS day
