from pyspark import SparkContext, StorageLevel
import json
from argparse import ArgumentParser

from digEmailExtractor.email_extractor import EmailExtractor
from digExtractor.extractor_processor import ExtractorProcessor

if __name__ == "__main__":
    sc = SparkContext()

    parser = ArgumentParser()
    parser.add_argument("-i", "--input", help="Input Folder", required=True)
    parser.add_argument("-o", "--output", help="Output Folder", required=True)
    parser.add_argument("-n", "--partitions", help="Number of partitions", required=False, default=20)
    parser.add_argument("-t", "--outputtype", help="Output file type - text or sequence", required=False, default="sequence")

    args = parser.parse_args()
    print ("Got arguments:", args)
    numPartitions = int(args.partitions)

    input_data = sc.sequenceFile(args.input).mapValues(lambda x: json.loads(x))\
            .partitionBy(numPartitions) \
            .persist(StorageLevel.MEMORY_AND_DISK)

    input_data.setName("input_data")

    email_extractor = EmailExtractor()\
            .set_metadata({'extractor': 'email'}) \
            .set_include_context(True) \
            .set_renamed_input_fields('text')

    email_extractor_processor = ExtractorProcessor() \
            .set_name('email_from_extracted_text-regex') \
            .set_input_fields('json_rep.postContent') \
            .set_output_field('extractions.email') \
            .set_extractor(email_extractor)

    output_rdd = input_data.mapValues(lambda x: email_extractor_processor.extract(x))
    output_rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(args.output)

