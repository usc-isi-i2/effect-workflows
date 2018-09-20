__author__ = 'dipsy'

import time
from argparse import ArgumentParser
import json
from datetime import datetime
from pyspark import SparkContext, SparkConf, StorageLevel
from digWorkflow.elastic_manager import ES
from hdfs.client import Client
import requests

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
    parser.add_argument("-m", "--hdfsManager", help="HDFS manager", required=True)
    parser.add_argument("-z", "--incremental", help="Incremental Run", required=False, action="store_true")
    parser.add_argument("-s", "--since", help="Get data since a timestamp - format: %Y-%m-%dT%H:%M:%S%Z", default="",
                        required=False)
    parser.add_argument("-w", "--addToIndex", help="Add to existing index", required=False, action="store_true")

    args = parser.parse_args()

    sc = SparkContext(appName="EFFECT-LOAD-TO-ES")
    conf = SparkConf()
    hdfs_client = Client(args.hdfsManager)
    hdfsRelativeFilname = args.input
    if hdfsRelativeFilname.startswith("hdfs://"):
        idx = hdfsRelativeFilname.find("/", 8)
        if idx != -1:
            hdfsRelativeFilname = hdfsRelativeFilname[idx:]

    create_index = True
    add_to_index = args.addToIndex
    inputFolder = args.input
    partitions = int(args.partitions)
    since = args.since.strip()
    if since == "initial":
        since = ""
    if args.incremental is True:
        if len(since) > 0:
            since = since[0:10]
            inputFolder = inputFolder + "/" + since
        else:
            inputFolder = inputFolder + "/initial"

    if args.doctype is None:
        document_types = hdfs_client.list(inputFolder, False)
    else:
        document_types = args.doctype.split(",")

    #If post is present, make it the first thing that loads as that is where we get issues
    if "post" in document_types:
        document_types.remove("post")
        document_types.insert(0, "post")

    #document_types = ["organization"]
    print "Got doc_types:", document_types


    def disable_index_refresh(es_write_conf, index):
        node = es_write_conf["es.nodes"].split(",")[0].strip()
        url = "http://" + node + ":" + es_write_conf["es.port"] + "/" + index + "/_settings"
        command = {
            "index": {
                "refresh_interval": "-1"
            }
        }
        print "Post:", url + ", data=" + json.dumps(command)
        ret = requests.put(url, data=json.dumps(command))
        return ret

    def enable_index_refresh(es_write_conf, index):
        node = es_write_conf["es.nodes"].split(",")[0].strip()
        url = "http://" + node + ":" + es_write_conf["es.port"] + "/" + index + "/_settings"
        command = {
            "index": {
                "refresh_interval": "30"
            }
        }
        print "Post:", url + ", data=" + json.dumps(command)
        ret = requests.put(url, data=json.dumps(command))
        return ret

    def delete_index(es_write_conf, index):
        node = es_write_conf["es.nodes"].split(",")[0].strip()
        url = "http://" + node + ":" + es_write_conf["es.port"] + "/" + index
        print "DELETE:", url
        ret = requests.delete(url)
        return ret

    # document_types = ["socialmedia"]
    for doc_type in document_types:
        doc_type = doc_type.strip()
        doc_type_folder = inputFolder + "/" + doc_type
        print "Add doc type:", doc_type, doc_type_folder

        input_rdd = sc.sequenceFile(doc_type_folder).repartition(partitions)

        if doc_type == 'topic' or doc_type == 'post':
            es_write_conf = {
                "es.nodes": args.host,
                "es.port": args.port,
                "es.nodes.discover": "false",
                'es.nodes.wan.only': "true",
                "es.resource": args.index + '/' + doc_type,  # use domain as `doc_type`
                "es.http.timeout": "60s",
                "es.http.retries": "20",
                "es.batch.write.retry.count": "20",  # maximum number of retries set
                "es.batch.write.retry.wait": "600s",  # on failure, time to wait prior to retrying
                "es.batch.size.entries": "500",  # number of docs per batch
                "es.mapping.id": "uri",  # use `uri` as Elasticsearch `_id`
                "es.input.json": "true"
            }
        elif doc_type == 'blog':
            es_write_conf = {
                "es.nodes": args.host,
                "es.port": args.port,
                "es.nodes.discover": "false",
                'es.nodes.wan.only': "true",
                "es.resource": args.index + '/' + doc_type,  # use domain as `doc_type`
                "es.http.timeout": "60s",
                "es.http.retries": "20",
                "es.batch.write.retry.count": "20",  # maximum number of retries set
                "es.batch.write.retry.wait": "600s",  # on failure, time to wait prior to retrying
                "es.batch.size.entries": "10000",  # number of docs per batch
                "es.mapping.id": "uri",  # use `uri` as Elasticsearch `_id`
                "es.input.json": "true"
            }
        elif doc_type == 'socialmedia':
            es_write_conf = {
                "es.nodes": args.host,
                "es.port": args.port,
                "es.nodes.discover": "false",
                'es.nodes.wan.only': "true",
                "es.resource": args.index + '/' + doc_type,  # use domain as `doc_type`
                "es.http.timeout": "60s",
                "es.http.retries": "20",
                "es.batch.write.retry.count": "20",  # maximum number of retries set
                "es.batch.write.retry.wait": "600s",  # on failure, time to wait prior to retrying
                "es.batch.size.entries": "2000000",  # number of docs per batch
                "es.mapping.id": "uri",  # use `uri` as Elasticsearch `_id`
                "es.input.json": "true"
            }
        else:
            es_write_conf = {
                "es.nodes": args.host,
                "es.port": args.port,
                "es.nodes.discover": "false",
                'es.nodes.wan.only': "true",
                "es.resource": args.index + '/' + doc_type,  # use domain as `doc_type`
                "es.http.timeout": "30s",
                "es.http.retries": "20",
                "es.batch.write.retry.count": "20",  # maximum number of retries set
                "es.batch.write.retry.wait": "300s",  # on failure, time to wait prior to retrying
                "es.batch.size.entries": "400000",  # number of docs per batch
                "es.mapping.id": "uri",  # use `uri` as Elasticsearch `_id`
                "es.input.json": "true"
            }

        print json.dumps(es_write_conf)

        es_manager = ES(sc, conf, es_write_conf=es_write_conf)
        if add_to_index is False:
            if create_index:
                delete_index(es_write_conf, args.index)
                es_manager.create_index(args.index,
                                        "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/es/es-mappings.json")
                create_index = False
                disable_index_refresh(es_write_conf, args.index)
        es_manager.rdd2es(input_rdd)

    enable_index_refresh({"es.nodes": args.host, "es.port": args.port}, args.index)

    es_manager_main = ES(sc, conf, es_write_conf={"es.nodes": args.host, "es.port": args.port})
    es_manager_main.create_alias("effect-data-{}".format(datetime.now().isoformat()[:10]), [args.index])
    if add_to_index is False:
        # Create alias effect to point to this new index
        es_manager_main.create_alias("effect", [args.index])
        es_manager_main.create_alias("effect-data-latest", [args.index])
