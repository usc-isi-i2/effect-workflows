ls /opt/cloudera/parcels/CDH/lib/spark/bin
mkdir bin
cp /opt/cloudera/parcels/CDH/lib/spark/bin/load-spark-env.sh bin
mv pyspark bin/
mkdir conf
grep -v PYSPARK_PYTHON /opt/cloudera/parcels/CDH/lib/spark/conf/spark-env.sh > conf/spark-env.sh

export PYSPARK_PYTHON=./effect-env-hbase.zip/effect-env/bin/python
export PYSPARK_DRIVER_PYTHON=./effect-env/effect-env/bin/python
export DEFAULT_PYTHON=./effect-env-hbase.zip/effect-env/bin/python
./bin/pyspark \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./effect-env-hbase.zip/effect-env/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./effect-env.-hbasezip/effect-env/bin/python \
  --conf spark.executorEnv.DEFAULT_PYTHON=./effect-env-hbase.zip/effect-env/bin/python \
  --conf spark.yarn.executor.memoryOverhead=7189 \
  --conf spark.kryoserializer.buffer.max=512m \
 --conf spark.driver.maxResultSize=0 \
  --master yarn \
   --deploy-mode client \
      --executor-memory 23g --num-executors 14 --executor-cores 4 \
     --jars "karma-spark-0.0.1-SNAPSHOT-1.6.0-cdh5.10.1-hive.jar" \
      --conf "spark.driver.extraClassPath=karma-spark-0.0.1-SNAPSHOT-1.6.0-cdh5.10.1-hive.jar" \
     --archives effect-env-hbase.zip,karma.zip \
    --files resources.zip,ner.params,hbase-site.xml \
     effectIncrementalWorkflow.py \
     $@