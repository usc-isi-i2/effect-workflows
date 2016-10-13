# Create table CDR
CREATE TABLE CDR(`_id` STRING, timestamp INT, raw_content STRING, content_type STRING, url STRING, version STRING, team STRING, source_name STRING)
COMMENT 'Used to store all CDR data'
PARTITIONED BY (year INT, month INT, day INT)
STORED AS SEQUENCEFILE;


# Create table for hackmagadden
CREATE TABLE hackmageddon (raw_content STRING)
STORED AS TEXTFILE

# Load data into hackmagadden
LOAD DATA INPATH '/user/effect/hackmageddon-cleaned.jl' INTO TABLE hackmageddon


# Load data into CDR from hackmagadden
FROM hackmageddon h
INSERT INTO TABLE cdr PARTITION(year='2016', month='10', day='10')
SELECT concat('hackmageddon/', hex(hash(h.raw_content))), unix_timestamp(), 
		h.raw_content, 'application/json', 
		concat('http://effect.isi.edu/input/hackmageddon/',hex(hash(h.raw_content))), 
		"2.0", "hackmageddon", "hackmageddon"


# Load the HyperionGray CVE Data
#1. Download data from API
#2. Convert JSON to Json Lines - jq -c .[] cve.json > cve.jl
#3. Upload data to hdfs
#4. Load into a hg_cve table
LOAD DATA INPATH '/user/effect/cve.jl' INTO TABLE hg_cve
#5. Insert data into CDR from hg_cve
FROM hg_cve h
INSERT INTO TABLE cdr PARTITION(year='2016', month='10', day='10')
SELECT concat('hg-cve/', hex(hash(h.raw_content))), unix_timestamp(), 
		h.raw_content, 'application/json', 
		concat('http://effect.isi.edu/input/hg/cve/',hex(hash(h.raw_content))), 
		"2.0", "hyperiongray", "hg-cve"

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
