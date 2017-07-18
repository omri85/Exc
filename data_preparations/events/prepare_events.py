""" Merge the events and load them to ES """
import csv
from elasticsearch import Elasticsearch

MERGED_EVENTS_FILE_NAME = "events_formatted.csv"

def merge_events():
    """ Merges start and end event to one record and write it to file """
    with open(MERGED_EVENTS_FILE_NAME, 'w') as output_file:
        with open(r"events.csv", 'r') as events_file:
            reader = csv.reader(events_file)
            raw_events = list(reader)
            start_events = [e for e in raw_events if e[2] == 'start']
            end_events = [e for e in raw_events if e[2] == 'end']
            for event in start_events:
                end_time = find_end_time(event, end_events)
                device_id = event[1]
                start_day = int(event[3][8:10])
                merged_event = [event[0], event[1], event[3], end_time]
                print >> output_file, "{},{},{},{}".format(event[0], event[1], event[3], end_time)

def split_events():
    """ Split the merged events file to file for each device """
    out1 = open('events1.csv', 'w')
    out2 = open('events2.csv', 'w')
    out3 = open('events3.csv', 'w')
    with open(MERGED_EVENTS_FILE_NAME, 'w') as ifile:
        reader = csv.reader(ifile)
        for event in reader:
            if (event[1] == "1"):
                print out1 >> event
            if (event[1] == "2"):
                print out2 >> event
            if (event[1] == "3"):
                print out3 >> event
    out1.close()
    out2.close()
    out3.close()

def load_events_to_es():
    es = Elasticsearch()
    for i in range(1,4):
        print i
        file_name = "events{}.csv".format(i)
        with open(file_name, 'r') as f:
            reader = csv.DictReader(f)
            helpers.bulk(es, reader, index=i, doc_type='event')

def main():
    merge_events()
    split_events()
    load_events_to_es()

if __name__ == '__main__':
    main()
