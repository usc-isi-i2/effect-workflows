/usr/lib/spark/bin/spark-submit --deploy-mode client \
      --executor-memory 7g --num-executors 5 --executor-cores 2 \
      --jars "karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
      --conf "spark.driver.extraClassPath=karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
      --py-files python-lib.zip \
      --archives karma.zip \
      effectWorkflow.py \
      $@




      ls /usr/lib/spark/bin
mkdir bin
cp /usr/lib/spark/bin/load-spark-env.sh bin
mv pyspark bin/
mkdir conf
grep -v PYSPARK_PYTHON /usr/lib/spark/conf/spark-env.sh > conf/spark-env.sh

export PYSPARK_PYTHON=./effect-env.zip/effect-env/bin/python
export PYSPARK_DRIVER_PYTHON=./effect-env/effect-env/bin/python
export DEFAULT_PYTHON=./effect-env.zip/effect-env/bin/python
./bin/pyspark \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./effect-env.zip/effect-env/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./effect-env.zip/effect-env/bin/python \
  --conf spark.executorEnv.DEFAULT_PYTHON=./effect-env.zip/effect-env/bin/python \
   --deploy-mode client \
      --executor-memory 7g --num-executors 5 --executor-cores 2 \
     --jars "karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
      --conf "spark.driver.extraClassPath=karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
     --archives effect-env.zip,karma.zip \
     effectWorkflow.py \
     $@