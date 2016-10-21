import json
import requests
from requests.auth import HTTPBasicAuth

class APIDownloader:
    def __init__(self, spark_context, sql_context):
        self.sc = spark_context
        self.sqlContext = sql_context

    def download_api(self, url, username=None, password=None):
        auth = None
        if username is not None:
            auth = HTTPBasicAuth(username, password)
        response = requests.get(url, verify=False,	auth=auth)
        return json.loads(response.text)

    def write_as_json_lines(self, array, file_handler):
        for line in array:
            json.dump(line, file_handler)
            file_handler.write("\n")


    def load_into_cdr(self, data, tablename):
        self.sqlContext.sql("DROP TABLE " + tablename)
        self.sqlContext.sql("CREATE TABLE " + tablename + "(raw_content STRING) STORED AS TEXTFILE")
        out_file = open(tablename + ".jl", "w")
        self.write_as_json_lines(data, out_file)
        out_file.close()
        self.sqlContext.sql("LOAD DATA INPATH '" + tablename + ".jl' INTO TABLE " + tablename)
