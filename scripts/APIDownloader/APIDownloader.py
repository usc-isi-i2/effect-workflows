import json
import requests
from requests.auth import HTTPBasicAuth

class APIDownloader:

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
