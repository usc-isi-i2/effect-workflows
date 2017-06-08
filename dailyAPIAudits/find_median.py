import sys
import logging
from itertools import groupby
from operator import itemgetter
import numpy as np
from collections import defaultdict
import datetime as dt
import scipy.stats as stats
SEP = '\t'
NULL = '\\N'

_logger = logging.getLogger(__name__)

def read_input(input_data):
    grouped_data=defaultdict(list)
    number_of_days_data=defaultdict(int)
    for line in input_data:
        source_name,count,number_of_days = line.strip().split(SEP)
        grouped_data[source_name].append(int(count))
        number_of_days_data[source_name]=int(number_of_days)
    return grouped_data,number_of_days_data

def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    try:
        data,num_days = read_input(sys.stdin)

        output=defaultdict()
        for source_name in data:
            values=[0]*(num_days[source_name]-len(data[source_name]))
            values.extend(data[source_name])
            output[source_name]=np.median(values)
        for source_name in output:
            print '\t'.join((source_name,str(output[source_name])))
    except:
        print sys.exc_info()
if __name__ == '__main__':
    main()