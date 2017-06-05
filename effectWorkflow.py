from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
import zipfile
import sys
from digSparkUtil.fileUtil import FileUtil
from cdrLoader import CDRLoader
from py4j.java_gateway import java_import
from digWorkflow.workflow import Workflow
from digWorkflow.git_model_loader import GitModelLoader
from digWorkflow.git_frame_loader import GitFrameLoader
from argparse import ArgumentParser

from digRegexExtractor.regex_extractor import RegexExtractor
from digExtractor.extractor_processor import ExtractorProcessor
import re
from hdfs.client import Client

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
save_intermediate_files = False

class EffectWorkflow(Workflow):
    def __init__(self, spark_context, sql_context, hdfs_client):
        self.sc = spark_context
        self.sqlContext = sql_context
        self.hdfsClient = hdfs_client

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
                if not hdfs_data_done(self.hdfsClient, outputFilename + '/models-out/' + model["name"]):
                    model_rdd = rdd.filter(lambda x: x[1]["source_name"] == model["name"])
                    if model["name"] == 'hg-taxii':
                        model_rdd = model_rdd.partitionBy(partitions * 4)
                    if not model_rdd.isEmpty():

                        print "Apply model for", model["name"], ":", model["url"]
                        #print json.dumps(model_rdd.first()[1])
                        karma_rdd = self.run_karma(model_rdd, model["url"], base_uri, model["root"], context_url,
                                                   num_partitions=partitions,
                                                   batch_size=1000,
                                                   additional_settings={
                                                   "karma.provenance.properties": "source,publisher,dateRecorded:date,observedDate:date",
                                                   # "karma.reducer.run": "false"
                                                   })
                        if not karma_rdd.isEmpty():
                            if save_intermediate_files is True:
                                fileUtil.save_file(karma_rdd, outputFilename + '/models-out/' + model["name"], "sequence",
                                               "json")
                            result_rdds.append(karma_rdd)
                else:
                    print "Loaded " + outputFilename + '/models-out/' + model["name"] + " from HDFS"
                    karma_rdd = self.sc.sequenceFile(outputFilename + '/models-out/' + model["name"]).mapValues(
                        lambda x: json.loads(x))
                    result_rdds.append(karma_rdd)

                print "Done applying model ", model["name"]

        if save_intermediate_files is True and not hdfs_data_done(self.hdfsClient, outputFilename + '/models-out/all'):
            all_done = self.sc.parallelize([{'a': 'Class', 'done': 'true'}]).map(lambda x: ("done", x))
            fileUtil.save_file(all_done, outputFilename + '/models-out/all', "text", "json")
        num_rdds = len(result_rdds)
        if num_rdds > 0:
            if num_rdds == 1:
                return result_rdds[0]
            return self.reduce_rdds_with_settings({"karma.provenance.properties": "source,publisher,dateRecorded:date,observedDate:date"},
                                                  outpartitions, *result_rdds)
        return None


def hdfs_data_done(hdfs_client, folder):
    data_done = False
    if folder.startswith("hdfs://"):
        idx = folder.find("/", 8)
        if idx != -1:
            folder = folder[idx:]

    if hdfs_client.content(folder, False):
        #Folder exists, check if it was created successfully
        if hdfs_client.content(folder + "/_SUCCESS", False):
            #There is success folder, so its already frames
            data_done = True
        else:
            #No success file, but folder exists, so delete the folder
            hdfs_client.delete(folder, recursive=True)

    print "Check folder exists:", folder, ":", data_done
    return data_done


def find(element, json):
    try:
        x = reduce(lambda d, key: d.get(key, {}), element.split("."), json)
        if x is not None:
            if any(x) is True:
                return x
    except:
        return None
    return None


def remove_blank_lines(json_data, attribute_name):
    clean_data = {}
    clean_data["data"] = json_data
    clean_data["success"] = False

    if json_data is not None:
        raw_text = find(attribute_name, json_data)
        if raw_text is not None:
            if type(raw_text) is list:
                clean_text = list()
                for raw_text_item in raw_text:
                    clean_text.append(' \n '.join([i.strip() for i in raw_text_item.split('\n') if len(i.strip()) > 0]))
            else:
                clean_text = ' \n '.join([i.strip() for i in raw_text.split('\n') if len(i.strip()) > 0])
            parts = attribute_name.split(".")
            attribute_end_name = parts[len(parts)-1]
            json_end = json_data
            for i in range(0, len(parts)-1):
                json_end = json_end[parts[i]]
            json_end[attribute_end_name] = clean_text

            clean_data["data"] = json_data
            clean_data["success"] = True
    return clean_data

if __name__ == "__main__":
    sc = SparkContext(appName="EFFECT-WORKFLOW")
    sqlContext = HiveContext(sc)

    java_import(sc._jvm, "edu.isi.karma")
    parser = ArgumentParser()
    parser.add_argument("-i", "--inputTable", help="Input Table", required=True)
    parser.add_argument("-o", "--output", help="Output Folder", required=True)
    parser.add_argument("-n", "--partitions", help="Number of partitions", required=False, default=20)
    parser.add_argument("-t", "--outputtype", help="Output file type - text or sequence", required=False,
                        default="sequence")
    parser.add_argument("-q", "--query", help="HIVE query to get data", default="", required=False)
    parser.add_argument("-k", "--karma", help="Run Karma", default=False, required=False, action="store_true")
    parser.add_argument("-f", "--framer", help="Run the framer", default=False, required=False, action="store_true")
    parser.add_argument("-m", "--hdfsManager", help="HDFS manager", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    fileUtil = FileUtil(sc)
    hdfs_client = Client(args.hdfsManager)#Config().get_client('dev')
    #sc._jsc.hadoopConfiguration()
    workflow = EffectWorkflow(sc, sqlContext, hdfs_client)

    inputTable = args.inputTable.strip()
    outputFilename = args.output.strip()
    outputFileType = args.outputtype.strip()
    hiveQuery = args.query.strip()
    #hiveQuery = "select * from CDR where source_name='hg-blogs' and raw_content like '%CVE-%'"
    # hiveQuery = "select * from CDR where source_name='asu-twitter'"
    numPartitions = int(args.partitions)

    numFramerPartitions = numPartitions / 2
    hdfsRelativeFilname = outputFilename
    if hdfsRelativeFilname.startswith("hdfs://"):
        idx = hdfsRelativeFilname.find("/", 8)
        if idx != -1:
            hdfsRelativeFilname = hdfsRelativeFilname[idx:]

    if not args.karma:
        reduced_rdd = sc.sequenceFile(outputFilename + "/reduced_rdd").mapValues(lambda x: json.loads(x)).persist(
            StorageLevel.MEMORY_AND_DISK)
    else:
        reduced_rdd_done = hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/reduced_rdd")

        if reduced_rdd_done is True:
            reduced_rdd = sc.sequenceFile(outputFilename + "/reduced_rdd").mapValues(lambda x: json.loads(x)).persist(
                StorageLevel.MEMORY_AND_DISK)
        else:
            # These are models without provenance, if neeed.
            # gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", "d24bbf5e11dd027ed91c26923035060432d93ab7")

            gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", "master")
            models = gitModelLoader.get_models_from_folder("models")

            print "Got models:", json.dumps(models)

            if not hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/models-out/all"):
                if not hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/cdr_extractions"):
                    if len(hiveQuery) > 0:
                        cdr_data = workflow.load_cdr_from_hive_query(hiveQuery) \
                            .partitionBy(numPartitions) \
                            .persist(StorageLevel.MEMORY_AND_DISK)
                    else:
                        cdr_data = workflow.load_cdr_from_hive_table(inputTable) \
                            .partitionBy(numPartitions)

                    # Add all extractors that work on the CDR data
                    cve_regex = re.compile('(cve-[0-9]{4}-[0-9]{4})', re.IGNORECASE)
                    cve_regex_extractor = RegexExtractor() \
                        .set_regex(cve_regex) \
                        .set_metadata({'extractor': 'cve-regex'}) \
                        .set_include_context(True) \
                        .set_renamed_input_fields('text')

                    cve_regex_extractor_processor = ExtractorProcessor() \
                        .set_name('cve_from_extracted_text-regex') \
                        .set_input_fields('raw_content') \
                        .set_output_field('extractions.cve') \
                        .set_extractor(cve_regex_extractor)

                    cdr_extractions_cve_rdd = cdr_data.mapValues(
                        lambda x: cve_regex_extractor_processor.extract(x)).persist(StorageLevel.MEMORY_AND_DISK)

                    from bbn.parameters import Parameters
                    params = Parameters('ner.params')
                    params.print_params()

                    zip_ref = zipfile.ZipFile(params.get_string('resources.zip'), 'r')
                    zip_ref.extractall()
                    zip_ref.close()


                    from bbn.ner_feature import NerFeature

                    ner_fea = NerFeature(params)

                    from bbn import decoder
                    from bbn.decoder import Decoder

                    def apply_bbn_extractor(data):
                        content_type = None
                        attribute_name = None

                        if data["source_name"] == 'hg-blogs':
                            content_type = "Blog"
                            attribute_name = "json_rep.text"
                        elif data["source_name"] == 'asu-twitter':
                            content_type = "SocialMediaPosting"
                            attribute_name = "json_rep.tweetContent"

                        if content_type is not None:
                            clean_data = remove_blank_lines(data, attribute_name)
                            if clean_data["success"] is True:
                                data = clean_data["data"]
                                return decoder.line_to_predictions(ner_fea, Decoder(params), data, attribute_name, content_type)
                        return data

                    cdr_extractions_rdd = cdr_extractions_cve_rdd.repartition(numPartitions*20)\
                            .mapValues(lambda x : apply_bbn_extractor(x))\
                            .repartition(numPartitions)\
                            .persist(StorageLevel.MEMORY_AND_DISK)

                    if save_intermediate_files is True:
                        fileUtil.save_file(cdr_extractions_rdd, outputFilename + '/cdr_extractions', outputFileType, "json")
                    #cdr_extractions_rdd.mapValues(lambda x: json.dumps(x)).map(lambda x: x[1]).saveAsTextFile(outputFilename + "/cdr_extractions_txt")
                else:
                    cdr_extractions_rdd = sc.sequenceFile(outputFilename + "/cdr_extractions").mapValues(
                        lambda x: json.loads(x)).partitionBy(numPartitions).persist(StorageLevel.MEMORY_AND_DISK)
                cdr_extractions_rdd.setName("cdr_extractions")

                # Run karma model as per the source of the data
                reduced_rdd = workflow.apply_karma_model_per_msg_type(cdr_extractions_rdd, models, context_url,
                                                                      base_uri,
                                                                      numPartitions, numFramerPartitions)\
                                        .persist(StorageLevel.MEMORY_AND_DISK)
            else:
                reduced_rdd = workflow.apply_karma_model_per_msg_type(None, models, context_url, base_uri,
                                                                      numPartitions, numFramerPartitions)\
                                        .persist(StorageLevel.MEMORY_AND_DISK)
            fileUtil.save_file(reduced_rdd, outputFilename + '/reduced_rdd', outputFileType, "json")

    # Frame the results
    if reduced_rdd is not None:
        if args.framer:
            reduced_rdd.setName("karma_out_reduced")

            gitFrameLoader = GitFrameLoader("usc-isi-i2", "effect-alignment", "master")
            all_frames = gitFrameLoader.get_frames_from_folder("frames")
            gitFrameLoader.load_context(context_url)
            types = gitFrameLoader.get_types_in_all_frames()

            frames = []
            # If there is a restart and the frames are already done, dont restart them
            for frame in all_frames:
                name = frame["name"]
                if not hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/frames/" + name):
                    frames.append(frame)

            type_to_rdd_json = workflow.apply_partition_on_types(reduced_rdd, types)

            for type_name in type_to_rdd_json:
                type_to_rdd_json[type_name]["rdd"] = type_to_rdd_json[type_name]["rdd"].persist(
                    StorageLevel.MEMORY_AND_DISK)
                type_to_rdd_json[type_name]["rdd"].setName(type_name)

            framer_output = workflow.apply_framer(reduced_rdd, type_to_rdd_json, frames,
                                                  numFramerPartitions,
                                                  None)

            for frame_name in framer_output:
                framer_output_one_frame = framer_output[frame_name]  #.coalesce(numFramerPartitions)
                print "Save frame:", frame_name
                if not framer_output_one_frame.isEmpty():
                    fileUtil.save_file(framer_output_one_frame, outputFilename + '/frames/' + frame_name, outputFileType,
                                       "json")

            reduced_rdd.unpersist()
            for type_name in type_to_rdd_json:
                type_to_rdd_json[type_name]["rdd"].unpersist()

