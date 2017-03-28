from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
import sys
from digSparkUtil.fileUtil import FileUtil
from cdrLoader import CDRLoader
from py4j.java_gateway import java_import
from digWorkflow.workflow import Workflow
from digWorkflow.git_model_loader import GitModelLoader
from argparse import ArgumentParser

from digRegexExtractor.regex_extractor import RegexExtractor
from digExtractor.extractor_processor import ExtractorProcessor
import re
from hdfs import Config

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

    def apply_karma_model_per_msg_type(self, rdd, models, context_url, base_uri, partitions, outpartitions):
        result_rdds = list()

        for model in models:
            if model["url"] != "":
                model_rdd = rdd.filter(lambda x: x[1]["source_name"] == model["name"])
                if not model_rdd.isEmpty():
                    print "Apply model for", model["name"], ":", model["url"]
                    #print json.dumps(model_rdd.first()[1])
                    karma_rdd = self.run_karma(model_rdd, model["url"], base_uri, model["root"], context_url,
                                               num_partitions=partitions,
                                               batch_size=1000,
                                               additional_settings={"karma.provenance.properties":"source,publisher,dateRecorded:date"})
                    if not karma_rdd.isEmpty():
                        #fileUtil.save_file(karma_rdd, outputFilename + '/' + model["name"], "text", "json")
                        result_rdds.append(karma_rdd)

                    print "Done applying model ", model["name"]

        num_rdds = len(result_rdds)
        if num_rdds > 0:
            if num_rdds == 1:
                return result_rdds[0]
            return self.reduce_rdds_with_settings({"karma.provenance.properties":"source,publisher,dateRecorded:date"}, outpartitions, *result_rdds)
        return None

if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = HiveContext(sc)

    java_import(sc._jvm, "edu.isi.karma")
    workflow = EffectWorkflow(sc, sqlContext)
    fileUtil = FileUtil(sc)
    hdfs_client = Config().get_client('dev')
    #sc._jsc.hadoopConfiguration()

    parser = ArgumentParser()
    parser.add_argument("-i", "--inputTable", help="Input Table", required=True)
    parser.add_argument("-o", "--output", help="Output Folder", required=True)
    parser.add_argument("-n", "--partitions", help="Number of partitions", required=False, default=20)
    parser.add_argument("-t", "--outputtype", help="Output file type - text or sequence", required=False, default="sequence")
    parser.add_argument("-q", "--query", help="HIVE query to get data", default="", required=False)
    parser.add_argument("-k", "--karma", help="Run Karma", default=False, required=False, action="store_true")
    parser.add_argument("-f", "--framer", help="Run the framer", default=False, required=False, action="store_true")

    args = parser.parse_args()
    print ("Got arguments:", args)

    inputTable = args.inputTable.strip()
    outputFilename = args.output.strip()
    outputFileType = args.outputtype.strip()
    hiveQuery = args.query.strip() #"select * from CDR where source_name='hg-blogs' and raw_content like '%CVE-%'"
    numPartitions = int(args.partitions)

    numFramerPartitions = numPartitions/2
    hdfsRelativeFilname = outputFilename
    if hdfsRelativeFilname.startswith("hdfs://"):
        idx = hdfsRelativeFilname.find("/", 8)
        if idx != -1:
            hdfsRelativeFilname = hdfsRelativeFilname[idx:]

    if not args.karma:
        reduced_rdd = sc.sequenceFile(outputFilename + "/reduced_rdd").mapValues(lambda x: json.loads(x)).persist(StorageLevel.MEMORY_AND_DISK)
    else:
        reduced_rdd_done = False
        if hdfs_client.content(hdfsRelativeFilname + "/reduced_rdd", False):
            #Folder exists, check if it was created successfully
            if hdfs_client.content(hdfsRelativeFilname + "/reduced_rdd/_SUCCESS", False):
                #There is success folder, so its already frames
                reduced_rdd_done = True
            else:
                #No success file, but folder exists, so delete the folder
                hdfs_client.delete(hdfsRelativeFilname + "/reduced_rdd")

        if reduced_rdd_done is True:
            reduced_rdd = sc.sequenceFile(outputFilename + "/reduced_rdd").mapValues(lambda x: json.loads(x)).persist(StorageLevel.MEMORY_AND_DISK)
        else:
            if len(hiveQuery) > 0:
                cdr_data = workflow.load_cdr_from_hive_query(hiveQuery)\
                    .partitionBy(numPartitions) \
                    .persist(StorageLevel.MEMORY_AND_DISK)
            else:
                cdr_data = workflow.load_cdr_from_hive_table(inputTable) \
                    .partitionBy(numPartitions)

            # Add all extractors that work on the CDR data
            cve_regex = re.compile('(cve-[0-9]{4}-[0-9]{4})', re.IGNORECASE)
            cve_regex_extractor = RegexExtractor()\
                    .set_regex(cve_regex) \
                    .set_metadata({'extractor': 'cve-regex'}) \
                    .set_include_context(True) \
                    .set_renamed_input_fields('text')

            cve_regex_extractor_processor = ExtractorProcessor() \
                    .set_name('cve_from_extracted_text-regex') \
                    .set_input_fields('json_rep.text') \
                    .set_output_field('extractions.cve') \
                    .set_extractor(cve_regex_extractor)

            cdr_extractions_rdd = cdr_data.mapValues(lambda x: cve_regex_extractor_processor.extract(x)).persist(StorageLevel.MEMORY_AND_DISK)
            cdr_extractions_rdd.setName("cdr_extractions")

            # These are models without provenance, if neeed.
            # gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", "d24bbf5e11dd027ed91c26923035060432d93ab7")

            gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", "master")
            models = gitModelLoader.get_models_from_folder("models")

            print "Got models:", json.dumps(models)
            # reduced_rdd = None
            # Run karma model as per the source of the data
            reduced_rdd = workflow.apply_karma_model_per_msg_type(cdr_extractions_rdd, models, context_url, base_uri,
                                                                      numPartitions, numFramerPartitions).persist(StorageLevel.MEMORY_AND_DISK)
            fileUtil.save_file(reduced_rdd, outputFilename + '/reduced_rdd', outputFileType, "json")

    # Frame the results
    if reduced_rdd is not None:
        if args.framer:
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
                {"name": "IPAddress", "uri": "http://schema.dig.isi.edu/ontology/IPAddress"},
                {"name": "ComputerHardware", "uri": "http://schema.dig.isi.edu/ontology/ComputerHardware"},
                {"name": "Blog", "uri": "http://schema.org/Blog"}
            ]

            all_frames = [
                {"name": "attack", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/attackevent.json"},
                {"name": "vulnerability", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/vulnerability.json"},
                {"name": "exploit", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/exploit.json"},
                #{"name": "topic", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/topic.json"},
                {"name": "post", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/post.json"},
                {"name": "malware", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/malware.json"},
                {"name": "blog", "url": "https://raw.githubusercontent.com/usc-isi-i2/effect-alignment/master/frames/blog.json"}
            ]

            frames = []
            # If there is a restart and the frames are already done, dont restart them
            for frame in all_frames:
                name = frame["name"]
                include_frame = True
                if hdfs_client.content(hdfsRelativeFilname + "/" + name, False):
                    #Folder exists, check if it was created successfully
                    if hdfs_client.content(hdfsRelativeFilname + "/" + name + "/_SUCCESS", False):
                        #There is success folder, so its already frames
                        include_frame = False
                    else:
                        #No success file, but folder exists, so delete the folder
                        hdfs_client.delete(hdfsRelativeFilname + "/" + name)
                if include_frame is True:
                    frames.append(frame)

            type_to_rdd_json = workflow.apply_partition_on_types(reduced_rdd, types)

            for type_name in type_to_rdd_json:
                type_to_rdd_json[type_name]["rdd"] = type_to_rdd_json[type_name]["rdd"].persist(StorageLevel.MEMORY_AND_DISK)
                type_to_rdd_json[type_name]["rdd"].setName(type_name)

            framer_output = workflow.apply_framer(reduced_rdd, type_to_rdd_json, frames,
                                                  numFramerPartitions,
                                                  None)

            for frame_name in framer_output:
                framer_output_one_frame = framer_output[frame_name] #.coalesce(numFramerPartitions)
                print "Save frame:", frame_name
                if not framer_output_one_frame.isEmpty():
                    fileUtil.save_file(framer_output_one_frame, outputFilename + '/' + frame_name, outputFileType, "json")

            reduced_rdd.unpersist()
            for type_name in type_to_rdd_json:
                type_to_rdd_json[type_name]["rdd"].unpersist()

    if args.karma:
        cdr_extractions_rdd.unpersist()