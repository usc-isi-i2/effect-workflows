__author__ = 'dipsy'

import time
from argparse import ArgumentParser
import json
from datetime import datetime
from pyspark import SparkContext, SparkConf, StorageLevel
from digWorkflow.elastic_manager import ES

'''
spark-submit --deploy-mode client  \
    --executor-memory 5g \
    --driver-memory 5g \
    --jars "/home/hadoop/effect-workflows/jars/elasticsearch-hadoop-2.4.0.jar" \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    /home/hadoop/effect-workflows/effectWorkflow-es.py \
    --host 172.31.19.102 \
    --port 9200 \
    --index effect-2 \
    --doctype attack \
    --input hdfs://ip-172-31-19-102/user/effect/data/cdr-framed/attack
'''

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-n", "--host", help="ES hostname", default="localhost", required=False)
    parser.add_argument("-p", "--port", help="ES port", default="9200", required=False)
    parser.add_argument("-x", "--index", help="ES Index name", required=True)
    parser.add_argument("-d", "--doctype", help="ES Document type", required=True)
    parser.add_argument("-t", "--partitions", help="Number of partitions", required=False, default=10)
    parser.add_argument("-i", "--input", help="Input Folder", required=True)

    args = parser.parse_args()

    sc = SparkContext(appName="EFFECT-LOAD-TO-ES")
    conf = SparkConf()

    input_rdd = sc.sequenceFile(args.input).partitionBy(args.partitions)

    es_write_conf = {
    "es.nodes" : args.host,
    "es.port" : args.port,
    "es.nodes.discover" : "false",
    'es.nodes.wan.only': "true",
    "es.resource" : args.index + '/' + args.doctype, # use domain as `doc_type`
    "es.http.timeout": "30s",
    "es.http.retries": "20",
    "es.batch.write.retry.count": "20", # maximum number of retries set
    "es.batch.write.retry.wait": "300s", # on failure, time to wait prior to retrying
    "es.batch.size.entries": "200000", # number of docs per batch
    "es.mapping.id": "uri", # use `uri` as Elasticsearch `_id`
    "es.input.json": "true"
    }

    print json.dumps(es_write_conf)

    es_manager = ES(sc, conf, es_write_conf=es_write_conf)
    es_manager.rdd2es(input_rdd)

