from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
import zipfile
import sys
from dateUtil import DateUtil
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
alias_models = {
    "asu-twitter": ["isi-twitter"],
    "isi-company-cpe": ["isi-company-cpe-linkedin"]
}

source_extraction_fields = {
    "hg-blogs": ["json_rep.text"],
    "isi-news": ["json_rep.readable_text"]
}


class EffectWorkflow(Workflow):
    def __init__(self, spark_context, sql_context, hdfs_client):
        self.sc = spark_context
        self.sqlContext = sql_context
        self.hdfsClient = hdfs_client

    def load_cdr_from_hive_query(self, query):
        cdr_data = self.sqlContext.sql(query)
        cdrLoader = CDRLoader()
        print "Running HIVE:", query
        return cdr_data.map(lambda x: cdrLoader.load_from_hive_row(x))

    def load_cdr_from_hive_table(self, table):
        return self.load_cdr_from_hive_query("FROM " + table + " SELECT *")

    def apply_karma_model_per_msg_type(self, rdd, models, context_url, base_uri, partitions, outpartitions, outputFilename, isIncremental, since):
        result_rdds = list()

        for model in models:
            if model["url"] != "":
                model_out_folder = outputFilename + '/models-out'
                if isIncremental is True:
                    if len(since) > 0:
                        model_out_folder = outputFilename  + '/models-out/' + since
                    else:
                        model_out_folder = outputFilename  + '/models-out/initial'

                if not hdfs_data_done(self.hdfsClient, model_out_folder + "/" + model["name"]):
                    alias_source_names = []
                    if model["name"] in alias_models:
                        alias_source_names = alias_models[model["name"]]
                    alias_source_names.append(model["name"])
                    model_rdd = rdd.filter(lambda x: x[1]["source_name"] in alias_source_names)
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
                                fileUtil.save_file(karma_rdd, model_out_folder + "/" + model["name"], "sequence",
                                               "json")
                            result_rdds.append(karma_rdd)
                else:
                    print "Loaded " + model_out_folder + "/" + model["name"] + " from HDFS"
                    karma_rdd = self.sc.sequenceFile(model_out_folder + "/" + model["name"]).mapValues(
                        lambda x: json.loads(x))
                    result_rdds.append(karma_rdd)

                print "Done applying model ", model["name"]

        if save_intermediate_files is True and not hdfs_data_done(self.hdfsClient,  model_out_folder + '/all'):
            all_done = self.sc.parallelize([{'a': 'Class', 'done': 'true'}]).map(lambda x: ("done", x))
            fileUtil.save_file(all_done, model_out_folder + '/all', "text", "json")

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
    parser.add_argument("-b", "--skipBBNExtractor", help="Skip BBN Extractor", required=False, action="store_true")

    parser.add_argument("-z", "--incremental", help="Incremental Run", required=False, action="store_true")
    parser.add_argument("-r", "--branch", help="Branch to pull models and frames from", required=False, default="master")
    parser.add_argument("-s", "--since", help="Get data since a timestamp - format: %Y-%m-%dT%H:%M:%S%Z", default="", required=False)

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
    isIncremental = args.incremental

    since = args.since.strip()
    if since == "initial":
        since = ""
    if len(since) > 0:
        timestamp = DateUtil.unix_timestamp(since, "%Y-%m-%dT%H:%M:%S%Z")/1000
        hiveQuery = "SELECT * from " + inputTable + " WHERE timestamp > " + str(timestamp)
        #hiveQuery = "SELECT * from cdr WHERE source_name='isi-twitter' or source_name='asu-twitter'"
        since = since[0:10]
    # hiveQuery = "select * from CDR where source_name='asu-hacking-items'"
    # hiveQuery = "select * from CDR where source_name='asu-twitter'"
    numPartitions = int(args.partitions)

    numFramerPartitions = numPartitions / 2
    hdfsRelativeFilname = outputFilename
    if hdfsRelativeFilname.startswith("hdfs://"):
        idx = hdfsRelativeFilname.find("/", 8)
        if idx != -1:
            hdfsRelativeFilname = hdfsRelativeFilname[idx:]

    if not args.karma:
        reduced_rdd_start = sc.sequenceFile(outputFilename + "/reduced_rdd").mapValues(lambda x: json.loads(x))
        reduced_rdd  = workflow.reduce_rdds_with_settings({"karma.provenance.properties": "source,publisher,dateRecorded:date,observedDate:date"},
                                                  numPartitions, reduced_rdd_start)\
                                .persist(StorageLevel.MEMORY_AND_DISK)
    else:
        if args.incremental is True:
            if len(since) > 0:
                reduced_rdd_done = hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/reduced_rdd/" + since)
            else:
                reduced_rdd_done = hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/reduced_rdd/initial")
        else:
            reduced_rdd_done = hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/reduced_rdd")

        if reduced_rdd_done is True:
            reduced_rdd_start = sc.sequenceFile(outputFilename + "/reduced_rdd").mapValues(lambda x: json.loads(x))
            reduced_rdd  = workflow.reduce_rdds_with_settings({"karma.provenance.properties": "source,publisher,dateRecorded:date,observedDate:date"},
                                                  numPartitions, reduced_rdd_start)\
                                .persist(StorageLevel.MEMORY_AND_DISK)
        else:
            # These are models without provenance, if neeed.
            # gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", "d24bbf5e11dd027ed91c26923035060432d93ab7")

            gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", args.branch, "/data1/github/effect-alignment")
            models = gitModelLoader.get_models_from_folder("models")
            print "Got models:", json.dumps(models)

            extractions_rdd_done = False
            if args.incremental is True:
                if len(since) > 0:
                    extractions_rdd_done = hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/cdr_extractions/" + since)
                else:
                    extractions_rdd_done = hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/cdr_extractions/initial")
            else:
                extractions_rdd_done = hdfs_data_done(hdfs_client, hdfsRelativeFilname + "/cdr_extractions")

            if extractions_rdd_done is False:
                if len(hiveQuery) > 0:
                    cdr_data = workflow.load_cdr_from_hive_query(hiveQuery) \
                        .repartition(numPartitions*20) \
                        .persist(StorageLevel.MEMORY_AND_DISK)
                else:
                    cdr_data = workflow.load_cdr_from_hive_table(inputTable) \
                        .repartition(numPartitions*20) \
                        .persist(StorageLevel.MEMORY_AND_DISK)
                #cdr_data.filter(lambda x: x[1]["source_name"] == "hg-blogs").mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename + "/blogs-input")
                #cdr_data.filter(lambda x: x[1]["source_name"] == "asu-twitter").mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename + "/tweets-input")

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

                msid_regex = re.compile('(ms[0-9]{2}-[0-9]{3})', re.IGNORECASE)
                msid_regex_extractor = RegexExtractor() \
                    .set_regex(msid_regex) \
                    .set_metadata({'extractor': 'msid-regex'}) \
                    .set_include_context(True) \
                    .set_renamed_input_fields('text')

                msid_regex_extractor_processor = ExtractorProcessor() \
                    .set_name('msid_from_extracted_text-regex') \
                    .set_input_fields('raw_content') \
                    .set_output_field('extractions.msid') \
                    .set_extractor(msid_regex_extractor)

                cdr_extractions_isi_rdd = sc.emptyRDD()
                extraction_source_names = []
                for source in source_extraction_fields:
                    extraction_source_names.append(source)
                    extraction_fields = source_extraction_fields[source]

                    cve_process_source = ExtractorProcessor() \
                                        .set_name('cve_from_extracted_text-regex') \
                                        .set_input_fields(extraction_fields) \
                                        .set_output_field('extractions.cve') \
                                        .set_extractor(cve_regex_extractor)
                    msid_process_source = ExtractorProcessor() \
                                        .set_name('msid_from_extracted_text-regex') \
                                        .set_input_fields(extraction_fields) \
                                        .set_output_field('extractions.msid') \
                                        .set_extractor(msid_regex_extractor)

                    cdr_extractions_isi_rdd_source = cdr_data.filter(lambda x: x[1]["source_name"] == source) \
                        .mapValues(lambda x: cve_process_source.extract(x))\
                        .mapValues(lambda x: msid_process_source.extract(x))
                    num_source = cdr_extractions_isi_rdd_source.count()
                    print "Got", num_source, " items for ", source
                    cdr_extractions_isi_rdd = cdr_extractions_isi_rdd.union(cdr_extractions_isi_rdd_source)
                    union_count = cdr_extractions_isi_rdd.count()
                    print "There are ", union_count, "total objects now"

                cdr_data_other = cdr_data.filter(lambda x: x[1]["source_name"] not in extraction_source_names)

                cdr_extractions_isi_rdd = cdr_extractions_isi_rdd.union(cdr_data_other\
                        .mapValues(lambda x: cve_regex_extractor_processor.extract(x))\
                        .mapValues(lambda x: msid_regex_extractor_processor.extract(x)))


                cdr_extractions_isi_rdd.persist(StorageLevel.MEMORY_AND_DISK)
                cdr_extractions_isi_rdd.setName("cdr_extractions-isi")
                count = cdr_extractions_isi_rdd.count()
                print "There are ", count, "total objects in ALL now"

                if args.skipBBNExtractor is True:
                    cdr_extractions_rdd = cdr_extractions_isi_rdd
                else:
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
                        elif data["source_name"] == 'asu-twitter' or data["source_name"] == 'isi-twitter':
                            content_type = "SocialMediaPosting"
                            attribute_name = "json_rep.tweetContent"
                        elif data["source_name"] == 'isi-news':
                            content_type = "NewsArticle"
                            attribute_name = "json_rep.readable_text"
                        elif data["source_name"] == 'asu-hacking-posts':
                            content_type = "Post"
                            attribute_name = "json_rep.postContent"

                        if content_type is not None:
                            clean_data = remove_blank_lines(data, attribute_name)
                            if clean_data["success"] is True:
                                data = clean_data["data"]
                                return decoder.line_to_predictions(ner_fea, Decoder(params), data, attribute_name, content_type)
                        return data

                    cdr_extractions_rdd = cdr_extractions_isi_rdd\
                            .mapValues(lambda x : apply_bbn_extractor(x))\
                            .repartition(numPartitions)\
                            .persist(StorageLevel.MEMORY_AND_DISK)

                    cdr_extractions_rdd.setName("cdr_extractions")

                if args.incremental is True:
                    if len(since) > 0:
                        fileUtil.save_file(cdr_extractions_rdd, outputFilename + '/cdr_extractions/' + since, outputFileType, "json")
                    else:
                        fileUtil.save_file(cdr_extractions_rdd, outputFilename + '/cdr_extractions/initial', outputFileType, "json")
                else:
                    fileUtil.save_file(cdr_extractions_rdd, outputFilename + '/cdr_extractions', outputFileType, "json")
            else:
                if args.incremental is True:
                    if len(since) > 0:
                        cdr_extractions_rdd = sc.sequenceFile(outputFilename + '/cdr_extractions/' + since).mapValues(lambda x: json.loads(x))
                    else:
                        cdr_extractions_rdd = sc.sequenceFile(outputFilename + '/cdr_extractions').mapValues(lambda x: json.loads(x)) #For initial, load everything
                else:
                    cdr_extractions_rdd = sc.sequenceFile(outputFilename + '/cdr_extractions').mapValues(lambda x: json.loads(x))

                cdr_extractions_rdd =  cdr_extractions_rdd.repartition(numPartitions*20) \
                                        .persist(StorageLevel.MEMORY_AND_DISK)

                cdr_extractions_rdd.setName("cdr_extractions")

            # Run karma model as per the source of the data
            reduced_rdd = None
            reduced_rdd = workflow.apply_karma_model_per_msg_type(cdr_extractions_rdd, models, context_url,
                                                                  base_uri,
                                                                  numPartitions, numFramerPartitions,
                                                                  outputFilename, isIncremental, since)\
                                    .persist(StorageLevel.MEMORY_AND_DISK)

            if args.incremental is True:
                if len(since) > 0:
                    fileUtil.save_file(reduced_rdd, outputFilename + '/reduced_rdd/' + since, outputFileType, "json")
                else:
                    fileUtil.save_file(reduced_rdd, outputFilename + '/reduced_rdd/initial', outputFileType, "json")
                if args.framer:
                    #If we also need to frame, we need to load entire set for framing
                    reduced_rdd_start = sc.sequenceFile(outputFilename + "/reduced_rdd").mapValues(lambda x: json.loads(x))
                    reduced_rdd  = workflow.reduce_rdds_with_settings({"karma.provenance.properties": "source,publisher,dateRecorded:date,observedDate:date"},
                                                      numPartitions, reduced_rdd_start)\
                                    .persist(StorageLevel.MEMORY_AND_DISK)
            else:
                fileUtil.save_file(reduced_rdd, outputFilename + '/reduced_rdd', outputFileType, "json")

            # Load the entire reduced_rdd for framer


    # Frame the results
    if reduced_rdd is not None:
        if args.framer:
            reduced_rdd.setName("karma_out_reduced")

            gitFrameLoader = GitFrameLoader("usc-isi-i2", "effect-alignment", args.branch, "/data1/github/effect-alignment")
            all_frames = gitFrameLoader.get_frames_from_folder("frames")
            gitFrameLoader.load_context(context_url)
            types = gitFrameLoader.get_types_in_all_frames()

            frames_folder = "/frames/"
            if args.incremental is True:
                if len(since) > 0:
                    frames_folder = frames_folder + since + "/"
                else:
                    frames_folder = frames_folder + "initial/"
            frames = []
            # If there is a restart and the frames are already done, dont restart them
            for frame in all_frames:
                name = frame["name"]
                if not hdfs_data_done(hdfs_client, hdfsRelativeFilname + frames_folder + name):
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
                    fileUtil.save_file(framer_output_one_frame, outputFilename + frames_folder + frame_name, outputFileType,
                                       "json")

            reduced_rdd.unpersist()
            for type_name in type_to_rdd_json:
                type_to_rdd_json[type_name]["rdd"].unpersist()

