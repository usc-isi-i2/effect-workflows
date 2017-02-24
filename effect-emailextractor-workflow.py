from pyspark import SparkContext, StorageLevel
import json
from argparse import ArgumentParser

from digEmailExtractor.email_extractor import EmailExtractor
from digExtractor.extractor_processor import ExtractorProcessor

'''
Workflow for running the Email Extractor.
It reads sequence file conatining CDR JSON data and outputs the extractions within the extractions attribute of the
JSON input data.

To execute this workflow on the command line, see run-extractor.sh
It assumes that spark is installed in /usr/lib/spark/bin
If not, you would need to change the paths in the shell script

'''

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

    # Read the input data
    input_data = sc.sequenceFile(args.input).mapValues(lambda x: json.loads(x))\
            .partitionBy(numPartitions) \
            .persist(StorageLevel.MEMORY_AND_DISK)
    input_data.setName("input_data")

    # Initialize the Email Extractor.
    # We set metadata that the extractor is an email extractor
    # Internally the extractors pass data in the 'text' field, so we set the
    # input field for the extractor to be text
    email_extractor = EmailExtractor()\
            .set_metadata({'extractor': 'email'}) \
            .set_include_context(True) \
            .set_renamed_input_fields('text')

    # Initialize the Extractor Processor, which is the engine that
    # runs the extractors. It has the logic for chaining extractors
    # if there are multiple ones that need to be run.
    # input fields - The json path to the field on which the extractor should be run.
    #               In this case, it is json_rep.postContent
    # output field - The json path to where the extractions should be placed within in input json
    #               In this example, we are putting them in extractions.email
    email_extractor_processor = ExtractorProcessor() \
            .set_name('email_from_extracted_text-regex') \
            .set_input_fields('json_rep.postContent') \
            .set_output_field('extractions.email') \
            .set_extractor(email_extractor)

    # Run the extractor on the json values
    output_rdd = input_data.mapValues(lambda x: email_extractor_processor.extract(x))

    # Save the output
    output_rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(args.output)

