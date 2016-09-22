__author__ = 'dipsy'

from pyspark import SparkContext
from pyspark.sql import HiveContext
import json
import sys
from digSparkUtil.fileUtil import FileUtil
from cdrLoader import CDRLoader

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
    cdrLoader = CDRLoader()

    inputTable = sys.argv[1]
    outputFilename = sys.argv[2]
    outputFileType = sys.argv[3]

    cdr_data = sqlContext.sql("FROM " + inputTable + " SELECT *")

    cdr_convert = cdr_data.map(lambda x: cdrLoader.load_from_hive_row(x))

    fileUtil.save_file(cdr_convert, outputFilename, outputFileType, "json")