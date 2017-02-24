ls /usr/lib/spark/bin
mkdir bin
cp /usr/lib/spark/bin/load-spark-env.sh bin
cp pyspark bin/
mkdir conf
grep -v PYSPARK_PYTHON /usr/lib/spark/conf/spark-env.sh > conf/spark-env.sh

export PYSPARK_DRIVER_PYTHON=./effect-env/bin/python

export PYSPARK_PYTHON=./effect-env.zip/effect-env/bin/python
export DEFAULT_PYTHON=./effect-env.zip/effect-env/bin/python
./bin/pyspark \
    --executor-memory 7g --num-executors 5 --executor-cores 2 \
    --archives ./effect-env.zip \
    effect-emailextractor-workflow.py \
    --input /user/effect/data/part-00000 \
    --output /user/effect/data/extractor-out-cmd