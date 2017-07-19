import os
from time import sleep
import sys
import csv
from elasticsearch import Elasticsearch, helpers

ES_PING_INTERVAL = 5
ES_PING_MAX_RETRIES = 24
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
                    "fielddata": True
                },
                "content_id":{
                    "type": "text"
                }
            }
        }
    }
}

def upload_final_data(es):
    """ Uploads the persons with content to ES. Index for every content id """
    attempt = 1
    while attempt <= ES_PING_MAX_RETRIES:
        print "Pinging ES cluster, attempt {}/{}".format(attempt, ES_PING_MAX_RETRIES)
        if es.ping():
            break
        sleep(5)
        attempt += 1
    else:
        print "Error: Failed to connect to ES"
        sys.exit(1)
    file_names = os.listdir(TEMP_FOLDER_PATH)
    print "looking in folder {}".format(TEMP_FOLDER_PATH)
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
    es = Elasticsearch()
    upload_final_data(es)

if __name__ == '__main__':
    main()
