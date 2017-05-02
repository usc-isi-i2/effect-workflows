__author__ = 'dipsy'

import time
from argparse import ArgumentParser
import json
from datetime import datetime
from pyspark import SparkContext, SparkConf, StorageLevel
from digWorkflow.elastic_manager import ES
from hdfs.client import Client

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
    --doctype attack,vulnerability \
    --input hdfs://ip-172-31-19-102/user/effect/data/cdr-framed
'''

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-n", "--host", help="ES hostname", default="localhost", required=False)
    parser.add_argument("-p", "--port", help="ES port", default="9200", required=False)
    parser.add_argument("-x", "--index", help="ES Index name", required=True)
    parser.add_argument("-d", "--doctype", help="ES Document types", required=False)
    parser.add_argument("-t", "--partitions", help="Number of partitions", required=False, default=20)
    parser.add_argument("-i", "--input", help="Input Folder", required=True)

    args = parser.parse_args()


    sc = SparkContext(appName="EFFECT-LOAD-TO-ES")
    conf = SparkConf()
    hdfs_client = Client("http://cloudmgr03.isi.edu:50070")
    hdfsRelativeFilname = args.input
    if hdfsRelativeFilname.startswith("hdfs://"):
        idx = hdfsRelativeFilname.find("/", 8)
        if idx != -1:
            hdfsRelativeFilname = hdfsRelativeFilname[idx:]

    if args.doctype is None:
        document_types = hdfs_client.list(args.input, False)
    else:
        document_types = args.doctype.split(",")

    create_index = True

    for doc_type in document_types:
        doc_type = doc_type.strip()
        input_rdd = sc.sequenceFile(args.input + "/" + doc_type) #.partitionBy(args.partitions)

        if doc_type == 'topic' or doc_type == 'post':
           es_write_conf = {
            "es.nodes" : args.host,
            "es.port" : args.port,
            "es.nodes.discover" : "false",
            'es.nodes.wan.only': "true",
            "es.resource" : args.index + '/' + doc_type, # use domain as `doc_type`
            "es.http.timeout": "60s",
            "es.http.retries": "20",
            "es.batch.write.retry.count": "20", # maximum number of retries set
            "es.batch.write.retry.wait": "600s", # on failure, time to wait prior to retrying
            "es.batch.size.entries": "50", # number of docs per batch
            "es.mapping.id": "uri", # use `uri` as Elasticsearch `_id`
            "es.input.json": "true"
            }
        else:
            es_write_conf = {
            "es.nodes" : args.host,
            "es.port" : args.port,
            "es.nodes.discover" : "false",
            'es.nodes.wan.only': "true",
            "es.resource" : args.index + '/' + doc_type, # use domain as `doc_type`
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
        if create_index:
            es_manager.create_index(args.index, "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/es/es-mappings.json")
            create_index = False
        es_manager.rdd2es(input_rdd)

    # Create alias effect to point to this new index
    es_manager_main = ES(sc, conf, es_write_conf={"es.nodes":args.host, "es.port":args.port})
    es_manager_main.create_alias("effect", ["effect-malware", args.index])