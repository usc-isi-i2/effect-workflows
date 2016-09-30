from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
import sys
from digSparkUtil.fileUtil import FileUtil
from cdrLoader import CDRLoader
from py4j.java_gateway import java_import
from digWorkflow.workflow import Workflow
from digWorkflow.git_model_loader import GitModelLoader

'''
spark-submit --deploy-mode client  \
    --jars "/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
    --conf "spark.driver.extraClassPath=/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    /home/hadoop/effect-workflows/effectWorkflow.py \
    cdr hdfs://ip-172-31-19-102/user/effect/data/cdr-out sequence 10
'''

context_url = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/master/versions/3.0/karma/karma-context.json"
base_uri = "http://effect.isi.edu/data/"

class EffectWorkflow(Workflow):
    def __init__(self, spark_context, sql_context):
        self.sc = spark_context
        self.sqlContext = sql_context

    def load_cdr_from_hive_query(self, query):
        cdr_data = self.sqlContext.sql(query)
        cdrLoader = CDRLoader()
        return cdr_data.map(lambda x: cdrLoader.load_from_hive_row(x))

    def load_cdr_from_hive_table(self, table):
        return self.load_cdr_from_hive_query("FROM " + table + " SELECT *")

    def apply_karma_model_per_msg_type(self, rdd, models, context_url, base_uri, partitions):
        result_rdds = list()

        for model in models:
            if model["url"] != "":
                model_rdd = rdd.filter(lambda x: x[1]["source_name"] == model["name"])
                if not model_rdd.isEmpty():
                    print "Apply model for", model["name"], ":", model["url"]

                    #print json.dumps(model_rdd.first()[1])

                    # Since we are not using the framer, we should enable json nesting to get complete objects
                    karma_rdd = self.run_karma(model_rdd, model["url"], base_uri, model["root"], context_url,
                                               num_partitions=partitions,
                                               additional_settings={"rdf.generation.disable.nesting": "false"})
                    if not karma_rdd.isEmpty():
                        result_rdds.append(karma_rdd)

                    print "Done applying model ", model["name"]

        num_rdds = len(result_rdds)
        if num_rdds > 0:
            if num_rdds == 1:
                return result_rdds[0]
            return self.reduce_rdds(*result_rdds)
        return None


if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = HiveContext(sc)

    java_import(sc._jvm, "edu.isi.karma")
    effectWorkflow = EffectWorkflow(sc, sqlContext)
    fileUtil = FileUtil(sc)

    gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", "master")
    models = gitModelLoader.get_models_from_folder("models")

    print "Got models:", json.dumps(models)

    inputTable = sys.argv[1]
    outputFilename = sys.argv[2]
    outputFileType = sys.argv[3]
    numPartitions = int(sys.argv[4])

    cdr_data = effectWorkflow.load_cdr_from_hive_table(inputTable)\
                    .partitionBy(numPartitions)\
                    .persist(StorageLevel.MEMORY_AND_DISK)
    cdr_data.setName("cdr_data")
    #fileUtil.save_file(cdr_data, outputFilename, outputFileType, "json")

    karma_out = effectWorkflow.apply_karma_model_per_msg_type(cdr_data, models, context_url, base_uri, numPartitions)
    if karma_out is not None:
        fileUtil.save_file(karma_out, outputFilename, outputFileType, "json")

    cdr_data.unpersist()