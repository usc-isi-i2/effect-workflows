__author__ = 'dipsy'

from pyspark.sql import SparkSession
import json
from digSparkUtil.fileUtil import FileUtil

if __name__ == "__main__":
    spark = SparkSession.builder.appName("effectWorkflow").getOrCreate()
    sc = spark.sparkContext

    inputTable = argv[1]
    outputFilename = argv[2]
    outputFileType = argv[3]

    fileUtil = FileUtil(sc)
    cdr_data = spark.sql("FROM " + inputTable + " SELECT *")

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
            json_rep['json_rep'] = json.load(x.raw_content)
        return json_rep


    cdr_convert = cdr_data.map(lambda x: analyze_cdr(x))
    fileUtil.save_file(cdr_convert, outputFilename, outputFileType, "json")
