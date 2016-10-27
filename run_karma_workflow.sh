spark-submit --deploy-mode client \
      --executor-memory 7g --num-executors 5 --executor-cores 2 \
      --jars "/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
      --conf "spark.driver.extraClassPath=/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
      --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
      --archives /home/hadoop/effect-workflows/karma.zip \
      /home/hadoop/effect-workflows/effectWorkflow.py \
      cdr hdfs://ip-172-31-19-102/$1 sequence 10