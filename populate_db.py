import os
import csv
from elasticsearch import Elasticsearch, helpers

TEMP_FOLDER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
PERSON_INDEX_MAPPINGS = {
    "mappings": {
        "person": {
            "properties": {
                "device_id":{
                    "type": "text",
                    },
                "start": {
                    "type": "date"
                },
                "end": {
                    "type": "date"
                },
                "age":{
                    "type": "long"
                },
                "gender":{
                    "type": "text",
                    "fielddata": true
                },
                "content_id":{
                    "type": "text"
                }
            }
        }
    }
}

def upload_final_data():
    """ Uploads the persons with content to ES. Index for every content id """
    file_names = os.listdir(TEMP_FOLDER_PATH)
    es = Elasticsearch()
    print "Uploading data to ES..."
    for file_name in file_names:
        file_path = os.path.join(TEMP_FOLDER_PATH, file_name)
        content_id = file_name.strip('output').strip('.csv')
        print "loading data for content: {}".format(content_id)
        es.indices.create(index=content_id, body=PERSON_INDEX_MAPPINGS)
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                helpers.bulk(es, reader, index=content_id, doc_type='person')
        except Exception as ex:
            print ex
    print "Done"

def main():
    upload_final_data()

if __name__ == '__main__':
    main()
