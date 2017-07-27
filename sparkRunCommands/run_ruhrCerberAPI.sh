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
  --conf "spark.yarn.executor.memoryOverhead=6141" \
  --master yarn[1] \
   --deploy-mode client \
     --archives effect-env.zip \
     ruhrCerberAPI.py \
    --output "ruhr_cerber" \
   --outputFolder /user/effect/data/hive-backup/$2 \
    --team "ruhr" \
    --password mhaWupyXVQcnkLEWXPrLBXgcnzgVBaEa \
    --date $1