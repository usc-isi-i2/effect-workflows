spark-submit --deploy-mode client  \
    --executor-memory 5g \
    --driver-memory 5g \
    --jars "/home/hadoop/effect-workflows/jars/elasticsearch-hadoop-2.4.0.jar" \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    /home/hadoop/effect-workflows/effectWorkflow-es.py \
    --host 172.31.19.102 \
    --port 9200 \
    --index $2 \
    --doctype attack \
    --input hdfs://ip-172-31-19-102/$1/attack

spark-submit --deploy-mode client  \
    --executor-memory 5g \
    --driver-memory 5g \
    --jars "/home/hadoop/effect-workflows/jars/elasticsearch-hadoop-2.4.0.jar" \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    /home/hadoop/effect-workflows/effectWorkflow-es.py \
    --host 172.31.19.102 \
    --port 9200 \
    --index $2 \
    --doctype vulnerability \
    --input hdfs://ip-172-31-19-102/$1/vulnerability