""" REST API Http server for Advertima events data """
from datetime import datetime
from flask import Flask, request, abort
from flask_restful import Resource, Api
from flask_restful import reqparse
from request_params import RequestParams
from elasticsearch import Elasticsearch, TransportError
from populate_db import upload_final_data

app = Flask(__name__)
api = Api(app)
es = Elasticsearch(host='es', port=9200, http_auth=('elastic', 'changeme'))

DATETIME_FORMAT_ES = "%Y-%m-%dT%H:%M:%S"
DATETIME_FORMAT_RESPONSE = "%Y-%m-%d %H:%M:%S"

def get_query(start, end, device_id):
    """ Returns query dict with start and end dates and filters by device is for ES search """
    start_str = datetime.strftime(start, DATETIME_FORMAT_ES)
    end_str = datetime.strftime(end, DATETIME_FORMAT_ES)
    query = {
        "query" : {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "start": {"lte": end_str}
                        }
                    },
                    {
                        "range": {
                            "end": {"gte": start_str}
                        }
                    },
                    {
                        "match" :{
                            "device_id" : device_id
                        }
                    }
                ]
            }
        }
    }
    return query

def get_base_response(params):
    """ Reutrns the base response JSON for api call
    :params: RequestParams"""
    return {
        "start": datetime.strftime(params.start, DATETIME_FORMAT_RESPONSE),
        "end": datetime.strftime(params.end, DATETIME_FORMAT_RESPONSE),
        "device_id": params.device_id,
        "content_id": params.content_id
    }

def get_params():
    """ Returns RequestParams with the request parameters if request is valid.
    Otherwise will return Bad Request response """
    params = RequestParams.parse(request.args)
    if not params.is_valid:
        return abort(400, params.get_error_messages()[0])
    return params

def make_es_request(content_id, query):
    """ Returns the response from ES for index of content_id and filtered by query
    :content_id: content id filter
    :query: dict with filter definitions"""
    try:
        res = es.search(index=content_id, body=query)
        return res
    except TransportError as error:
        if error.error == "index_not_found_exception":
            return abort(400, "Invalid content id [{}]".format(content_id))
        else:
            abort(500, "Error accessing database")

class ViewerCount(Resource):
    """ Class of REST API resource view-count """
    def get(self):
        """ GET http method """
        params = get_params()
        query = get_query(params.start, params.end, params.device_id)
        res = make_es_request(params.content_id, query)
        views = res["hits"]["total"]
        response_json = get_base_response(params)
        response_json.update({"views" : views})
        return response_json

class AvgAge(Resource):
    """ Class of REST API resource avg-age """
    def get(self):
        """ GET http method """
        params = get_params()
        query = get_query(params.start, params.end, params.device_id)
        aggs = {
            "aggs":{
                "avg_age": {
                    "avg": {"field": "age"}
                }
            }
        }
        query.update(aggs)
        res = make_es_request(params.content_id, query)
        avg_age = res["aggregations"]["avg_age"]["value"]
        response_json = get_base_response(params)
        response_json.update({"avg_age" : round(avg_age, 1) if avg_age else None})
        return response_json

class GenderDist(Resource):
    """ Class of REST API resource gender-dist """
    def get(self):
        """ GET http method """
        params = get_params()
        query = get_query(params.start, params.end, params.device_id)
        aggs = {
            "aggs": {
                "genders" : {
                    "terms": {"field" : "gender"}
                }
            }
        }
        query.update(aggs)
        res = make_es_request(params.content_id, query)
        buckets = res["aggregations"]["genders"]["buckets"]
        male_bucket = [bucket for bucket in buckets if bucket["key"] == "male"]
        female_bucket = [bucket for bucket in buckets if bucket["key"] == "female"]
        male_count = male_bucket[0]["doc_count"] if male_bucket else 0
        female_count = female_bucket[0]["doc_count"] if female_bucket else 0
        total = float(male_count + female_count)
        male_dist, female_dist = (male_count / total, female_count / total) if total else (0.0, 0.0)
        response_json = get_base_response(params)
        response_json.update({"gender-dist" : {"male": round(male_dist, 2), "female": round(female_dist, 2)}})
        return response_json

api.add_resource(ViewerCount, '/viewer-count')
api.add_resource(AvgAge, '/avg-age')
api.add_resource(GenderDist, '/gender-dist')

if __name__ == '__main__':
    print "Uploading data to ES. Server will start when done..."
    upload_final_data(es)
    print "Data loading is done, starting server"
    app.run(host='0.0.0.0', port=5000)
