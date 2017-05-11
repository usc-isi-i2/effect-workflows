from pyspark import SparkContext, StorageLevel, SparkConf
import json
from digSparkUtil.fileUtil import FileUtil
from py4j.java_gateway import java_import
from digWorkflow.workflow import Workflow
from argparse import ArgumentParser
from digWorkflow.elastic_manager import ES

'''
spark-submit --deploy-mode client  \
    --jars "/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
    --conf "spark.driver.extraClassPath=/home/hadoop/effect-workflows/lib/karma-spark-0.0.1-SNAPSHOT-shaded.jar" \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    --archives /home/hadoop/effect-workflows/karma.zip \
    /home/hadoop/effect-workflows/effectWorkflow.py \
    cdr hdfs://ip-172-31-19-102/user/effect/data/cdr-framed sequence 10
'''

context_url = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/karma/karma-context.json"
base_uri = "http://effect.isi.edu/data/"

if __name__ == "__main__":
    sc = SparkContext()
    conf = SparkConf()

    java_import(sc._jvm, "edu.isi.karma")
    workflow = Workflow(sc)
    fileUtil = FileUtil(sc)

    parser = ArgumentParser()
    parser.add_argument("-i", "--input", help="input folder", required=True)
    parser.add_argument("-o", "--output", help="input folder", required=True)
    parser.add_argument("-n", "--partitions", help="Number of partitions", required=False, default=20)
    parser.add_argument("-t", "--host", help="ES hostname", default="localhost", required=False)
    parser.add_argument("-p", "--port", help="ES port", default="9200", required=False)
    parser.add_argument("-x", "--index", help="ES Index name", required=True)
    parser.add_argument("-d", "--doctype", help="ES Document types", required=True)
    args = parser.parse_args()
    print ("Got arguments:", args)

    input = args.input.strip()
    numPartitions = int(args.partitions)
    doc_type = args.doctype.strip()
    outputFilename = args.output.strip()
    numFramerPartitions = numPartitions/2
    inputRDD = workflow.batch_read_csv(input)
    outputFileType = "sequence"

    #2. Apply the karma Model
    reduced_rdd = workflow.run_karma(inputRDD,
                                   "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/models/ransonware/ransomware-model.ttl",
                                   "http://effect.isi.edu/data/",
                                   "http://schema.dig.isi.edu/ontology/Malware1",
                                   "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/karma/karma-context.json",
                                   num_partitions=numPartitions,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":","})
    if reduced_rdd is not None:
        reduced_rdd = reduced_rdd.persist(StorageLevel.MEMORY_AND_DISK)
        fileUtil.save_file(reduced_rdd, outputFilename + '/reduced_rdd', "sequence", "json")
        reduced_rdd.setName("karma_out_reduced")

        types = [
            {"name": "AttackEvent", "uri": "http://schema.dig.isi.edu/ontology/AttackEvent"},
            {"name": "EmailAddress", "uri": "http://schema.dig.isi.edu/ontology/EmailAddress"},
            {"name": "GeoCoordinates", "uri": "http://schema.org/GeoCoordinates"},
            {"name": "Organization", "uri": "http://schema.org/Organization"},
            {"name": "PersonOrOrganization", "uri": "http://schema.dig.isi.edu/ontology/PersonOrOrganization"},
            {"name": "PhoneNumber", "uri": "http://schema.dig.isi.edu/ontology/PhoneNumber"},
            {"name": "Place", "uri": "http://schema.org/Place"},
            {"name": "PostalAddress", "uri": "http://schema.org/PostalAddress"},
            {"name": "Vulnerability", "uri": "http://schema.dig.isi.edu/ontology/Vulnerability"},
            {"name": "SoftwareSystem", "uri":"http://schema.dig.isi.edu/ontology/SoftwareSystem"},
            {"name": "Identifier", "uri":"http://schema.dig.isi.edu/ontology/Identifier"},
            {"name": "CVSS", "uri":"http://schema.dig.isi.edu/ontology/CVSS"},
            {"name": "PriceSpecification", "uri": "http://schema.dig.isi.edu/ontology/PriceSpecification"},
            {"name": "Exploit", "uri": "http://schema.dig.isi.edu/ontology/Exploit"},
            {"name": "LoginCredentials", "uri": "http://schema.dig.isi.edu/ontology/LoginCredentials"},
            {"name": "UserName", "uri": "http://schema.dig.isi.edu/ontology/UserName"},
            {"name": "Password", "uri": "http://schema.dig.isi.edu/ontology/Password"},
            {"name": "Topic", "uri":"http://schema.dig.isi.edu/ontology/Topic"},
            {"name": "Post", "uri": "http://schema.dig.isi.edu/ontology/Post"},
            {"name": "Forum", "uri": "http://schema.dig.isi.edu/ontology/Forum"},
            {"name": "Malware", "uri": "http://schema.dig.isi.edu/ontology/Forum"},
            {"name": "IPAddress", "uri": "http://schema.dig.isi.edu/ontology/IPAddress"}
        ]

        frames = [
           {"name": "malware", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/malware.json"}
        ]
        type_to_rdd_json = workflow.apply_partition_on_types(reduced_rdd, types)

        for type_name in type_to_rdd_json:
            type_to_rdd_json[type_name]["rdd"] = type_to_rdd_json[type_name]["rdd"].persist(StorageLevel.MEMORY_AND_DISK)
            type_to_rdd_json[type_name]["rdd"].setName(type_name)

        framer_output = workflow.apply_framer(reduced_rdd, type_to_rdd_json, frames,
                                              numFramerPartitions,
                                              None)

        for frame_name in framer_output:
            framer_output_one_frame = framer_output[frame_name].coalesce(numFramerPartitions)
            if not framer_output_one_frame.isEmpty():
                fileUtil.save_file(framer_output_one_frame, outputFilename + '/' + frame_name, outputFileType, "json")
