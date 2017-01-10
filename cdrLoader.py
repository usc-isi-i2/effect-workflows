#from xmljson import yahoo as xmlConvertor
from xml.etree.ElementTree import fromstring
import json

class CDRLoader:

    def load_from_json_object(self, json_object):
        json_rep = {}
        for column_name in json_object:
            json_rep[column_name] = json_object[column_name]

        json_rep['json_rep'] = self.generate_json_rep(json_rep['content_type'],
                                                      json_rep['raw_content'])
        return json_rep['_id'], json_rep

    def load_from_hive_row(self, row):
        json_rep = {}
        json_rep['_id'] = row._id
        json_rep['timestamp'] = row.timestamp
        json_rep['raw_content'] = row.raw_content
        json_rep['content_type'] = row.content_type
        json_rep['url'] = row.url
        json_rep['version'] = row.version
        json_rep['team'] = row.team
        json_rep['source_name'] = row.source_name
        json_rep['json_rep'] = self.generate_json_rep(row.content_type, row.raw_content)
        return row._id, json_rep

    def generate_json_rep(self, content_type, raw_content):
        if content_type == "application/json":
            return json.loads(raw_content)
        # The xml convertor requires python 2.7 and up. Commented for now as we are using python 2.6
        # elif content_type == "application/xml":
        #     return xmlConvertor.data(fromstring(raw_content))
        return None
