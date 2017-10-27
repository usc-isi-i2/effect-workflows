spark-submit --master yarn --deploy-mode client  \
    --driver-memory 10g \
  --executor-memory 24g --num-executors 30 --executor-cores 2 \
    --jars "elasticsearch-hadoop-2.4.0.jar" \
    --py-files python-lib.zip \
    effectWorkflow-es.py \
    --host "128.9.35.71, 128.9.35.72, 128.9.35.73, 128.9.35.74" \
    --port 9200 \
    --hdfsManager "http://cloudmgr03.isi.edu:50070" \
    $@