from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from digSparkUtil.fileUtil import FileUtil
from py4j.java_gateway import java_import
from argparse import ArgumentParser
from effectWorkflow import EffectWorkflow

if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = HiveContext(sc)

    java_import(sc._jvm, "edu.isi.karma")
    workflow = EffectWorkflow(sc, sqlContext)
    fileUtil = FileUtil(sc)

    parser = ArgumentParser()
    parser.add_argument("-i", "--inputTable", help="Input Table", required=True)
    parser.add_argument("-o", "--output", help="Output Folder", required=True)
    parser.add_argument("-n", "--partitions", help="Number of partitions", required=False, default=20)
    parser.add_argument("-t", "--outputtype", help="Output file type - text or sequence", required=False, default="sequence")
    parser.add_argument("-q", "--query", help="HIVE query to get data", default="", required=False)

    args = parser.parse_args()
    print ("Got arguments:", args)

    inputTable = args.inputTable.strip()
    outputFilename = args.output.strip()
    outputFileType = args.outputtype.strip()
    hiveQuery = args.query.strip()
    numPartitions = int(args.partitions)

    numFramerPartitions = numPartitions/2

    if len(hiveQuery) > 0:
        cdr_data = workflow.load_cdr_from_hive_query(hiveQuery)\
            .partitionBy(numPartitions) \
            .persist(StorageLevel.MEMORY_AND_DISK)
    else:
        cdr_data = workflow.load_cdr_from_hive_table(inputTable) \
            .partitionBy(numPartitions) \
            .persist(StorageLevel.MEMORY_AND_DISK)

    cdr_data.setName("cdr_data")

    fileUtil.save_file(cdr_data, outputFilename, outputFileType, "json")