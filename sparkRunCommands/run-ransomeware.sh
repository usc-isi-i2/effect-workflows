spark-submit \
   --master yarn \
    --deploy-mode client \
    --jars "elasticsearch-hadoop-2.4.0.jar,karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
   --conf "spark.driver.extraClassPath=karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
    --py-files python-lib.zip \
      --archives karma.zip \
    ransomware-workflow.py \
    --host 128.9.35.71 \
    --port 9200 \
    --index effect-ransomeware \
    --doctype malware \
    --input $1 \
    --output $2