ls /opt/cloudera/parcels/CDH/lib/spark/bin
mkdir bin
cp /opt/cloudera/parcels/CDH/lib/spark/bin/load-spark-env.sh bin
mv pyspark bin/
mkdir conf
grep -v PYSPARK_PYTHON /opt/cloudera/parcels/CDH/lib/spark/conf/spark-env.sh > conf/spark-env.sh

export PYSPARK_PYTHON=./effect-env.zip/effect-env/bin/python
export PYSPARK_DRIVER_PYTHON=./effect-env/effect-env/bin/python
export DEFAULT_PYTHON=./effect-env.zip/effect-env/bin/python
./bin/pyspark \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./effect-env.zip/effect-env/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./effect-env.zip/effect-env/bin/python \
  --conf spark.executorEnv.DEFAULT_PYTHON=./effect-env.zip/effect-env/bin/python \
  --master yarn \
   --deploy-mode client \
      --executor-memory 25g --num-executors 12 --executor-cores 2 \
     --jars "karma-spark-0.0.1-SNAPSHOT-1.6.0-cdh5.10.1-hive.jar" \
      --conf "spark.driver.extraClassPath=karma-spark-0.0.1-SNAPSHOT-1.6.0-cdh5.10.1-hive.jar" \
     --archives effect-env.zip,karma.zip \
     effectWorkflow.py \
     --hdfsManager "http://cloudmgr03.isi.edu:50070" \
     $@