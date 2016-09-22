__author__ = 'dipsy'

from pyspark import SparkContext
from pyspark.sql import HiveContext
import json
import sys
from digSparkUtil.fileUtil import FileUtil

'''
spark-submit --deploy-mode client  \
        --py-files /home/hadoop/effect-env.zip \
        /home/hadoop/effect-workflows/effectWorkflow.py \
        cdr hdfs://ip-172-31-19-102//user/effect/data/cdr-out text

'''

if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = HiveContext(sc)
    fileUtil = FileUtil(sc)

    inputTable = sys.argv[1]
    outputFilename = sys.argv[2]
    outputFileType = sys.argv[3]

    cdr_data = sqlContext.sql("FROM " + inputTable + " SELECT *")
    #for x in cdr_data.collect():
    #    print x

    def analyze_cdr(x):
        json_rep = {}
        json_rep['_id'] = x._id
        json_rep['timestamp'] = x.timestamp
        json_rep['raw_content'] = x.raw_content
        json_rep['content_type'] = x.content_type
        json_rep['url'] = x.url
        json_rep['version'] = x.version
        json_rep['team'] = x.team
        if json_rep['content_type'] == 'application/json':
            json_rep['json_rep'] = json.loads(x.raw_content)
        return x._id, json_rep

    cdr_convert = cdr_data.map(lambda x: analyze_cdr(x))
    fileUtil.save_file(cdr_convert, outputFilename, outputFileType, "json")