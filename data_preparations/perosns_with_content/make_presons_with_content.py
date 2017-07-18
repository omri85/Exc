""" Create persons records with content id """
import os
from multiprocessing import JoinableQueue as Queue
from datetime import datetime
import csv
from threading import Thread
from elasticsearch import Elasticsearch
es = Elasticsearch()

DATETIME_FORMAT_CSV = "%Y-%m-%d %H:%M:%S"
DATETIME_FORMAT_ES = "%Y-%m-%dT%H:%M:%S"
NUMBER_OF_CALC_THREADS = 6
TEMP_FOLDER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, "data")
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
PERSONS_WITH_CONTENT_FILE_NAME = "persons_with_content.csv"

def get_events_for_person(person_record):
    """ Gets the events in the interval between person appeared and disapperead times.
    Notice that The event might started before or ended after. """ 
    device_id, appeared_str, disappeared_str, age, gender = person_record
    appeared = datetime.strptime(appeared_str, DATETIME_FORMAT_CSV)
    disappeared = datetime.strptime(disappeared_str, DATETIME_FORMAT_CSV)
    query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "start": {
                                "lte": disappeared_str.replace(" ", "T")
                            }
                        }
                    },
                    {
                        "range": {
                            "end": {
                                "gte": appeared_str.replace(" ", "T")
                            }
                        }
                    }
                ]
            }
        }
    }
    res = es.search(index=device_id, body=query)
    return [rec['_source'] for rec in res['hits']['hits']]

def split_person_record(person_record, events):
    """ Retrun a list of split of every person record to one or more records,
    each contains the content id which that person watched """
    res = []
    for event in events:
        event_start_time = datetime.strptime(event["start"], DATETIME_FORMAT_ES)
        event_end_time = datetime.strptime(event["end"], DATETIME_FORMAT_ES)
        appeared = datetime.strptime(person_record[1], DATETIME_FORMAT_CSV)
        disappeared = datetime.strptime(person_record[2], DATETIME_FORMAT_CSV)
        start = max(event_start_time, appeared)
        end = min(event_end_time, disappeared)
        res.append([person_record[0], datetime.strftime(start, DATETIME_FORMAT_ES),\
         datetime.strftime(end, DATETIME_FORMAT_ES), person_record[3], person_record[4], event["content_id"]])
    return res

def make_records():
    """ Splits every person record from file to records with content id
    and writes the results to file """
    persons_queue = Queue()
    output_queue = Queue()
    print "Reading persons from file..."
    with open("persons.csv", 'r') as f:
        reader = csv.reader(f)
        for record in reader:
            persons_queue.put(record)
    print "Done"
    print "Start calculating, using {} threads".format(NUMBER_OF_CALC_THREADS)
    for i in range(NUMBER_OF_CALC_THREADS):
        worker = CalcWorker(persons_queue, output_queue)
        worker.daemon = True
        worker.start()
    output_file = open(PERSONS_WITH_CONTENT_FILE_NAME, 'w')
    writer = WriterWorker(output_queue, output_file)
    writer.daemon = True
    writer.start()

    persons_queue.join()
    print "Done calculating"
    output_queue.join()
    print "Done writing to file"
    output_file.close()

class CalcWorker(Thread):
    """ Worker thread for executing calculation logic of an event class """
    def __init__(self, persons_queue, output_queue):
        Thread.__init__(self)
        self.persons_queue = persons_queue
        self.output_queue = output_queue

    def run(self):
        while True:
            person_record = self.persons_queue.get()
            try:
                events = get_events_for_person(person_record)
                res = split_person_record(person_record, events)
                for rec in res:
                    self.output_queue.put(rec)
            except Exception as ex:
                print ex.message
            finally:
                self.persons_queue.task_done()

class WriterWorker(Thread):
    """ Worker thread for executing calculation logic of an event class """
    def __init__(self, output_queue, output_file):
        Thread.__init__(self)
        self.output_file = output_file
        self.output_queue = output_queue

    def run(self):
        while True:
            output_record = self.output_queue.get()
            try:
                print >> self.output_file, ",".join(output_record)
            except Exception as ex:
                print ex.message
            finally:
                self.output_queue.task_done()

def split_final_data():
    """ Splits the persons with records file to file for every content id """
    os.mkdir(TEMP_FOLDER_PATH)
    files = dict()
    print "Splitting data..."
    with open(PERSONS_WITH_CONTENT_FILE_NAME, 'r') as data_file:
        reader = csv.reader(data_file)
        for record in reader:
            content_id = record[5]
            file_path = os.path.join(TEMP_FOLDER_PATH, "output{}.csv".format(content_id))
            if content_id not in files:
                f = open(file_path, 'w')
                files[content_id] = f
                print >> f, "device_id,start,end,age,gender,content_id"
            else:
                f = files[content_id]
            print >> f, ','.join(record)
    for f in files.values():
        f.close()
    print "Done"

def main():
    make_records()
    split_final_data()

if __name__ == '__main__':
    main()
