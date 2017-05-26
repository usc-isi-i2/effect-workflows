SET hive.exec.parallel=true;
SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;
SET hive.auto.convert.join=true;

INSERT INTO table daily_audit_irregularities
SELECT a.source_name,a.count,concat('(threshold: ',CAST(a.threshold AS STRING),')') FROM (
SELECT
a.source_name,
0 AS count, a.threshold FROM
(SELECT source_name,CAST(PERCENTILE(a.count,0.5) AS INT) AS threshold FROM daily_audit_data a GROUP BY a.source_name) a
FULL OUTER JOIN
todays_count b ON
a.source_name=b.source_name
WHERE b.source_name is null
UNION ALL
SELECT b.source_name, b.count, a.threshold FROM
(SELECT source_name,CAST(PERCENTILE(a.count,0.5) AS INT) AS threshold FROM daily_audit_data a GROUP BY a.source_name) a
JOIN
todays_count b ON
a.source_name=b.source_name
WHERE b.count<a.threshold)a;
