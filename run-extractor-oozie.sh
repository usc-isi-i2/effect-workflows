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
     --archives effect-env.zip \
    effect-emailextractor-workflow.py \
      --input $1 \
     --output $2