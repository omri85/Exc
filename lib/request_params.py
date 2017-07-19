""" Request paramteres class for Advertima REST API """
from datetime import datetime

DATETIME_FORMAT_REQUEST = "%Y-%m-%d %H:%M:%S"
MISSING_PARAM_FORAMT = "Missing mantdatory paramaeters: {}"
INVALID_DATE_FORMAT = "Invaild date format for parameter: {}. Expected " + DATETIME_FORMAT_REQUEST

class RequestParams(object):
    """ Requests parameters for REST api server model """
    def __init__(self):
        self.start = None
        self.end = None
        self.device_id = None
        self.content_id = None
        self.__error_messages = []

    def add_model_error(self, error_message):
        """ Adds an error and makes model not valid
        :error_message: error message string"""
        self.__error_messages.append(error_message)
    
    def is_valid(self):
        """ Return True if the model is valid """
        return not self.__error_messages

    def get_error_messages(self):
        """ Return all the error messages raised when parsing request args """
        return self.__error_messages

    @staticmethod
    def parse(args):
        """ Parses request args dict and returns RequestParams class
        :args: request args dict"""
        request_params = RequestParams()
        missing_params = []
        datetime_format_errors = []
        if "start" in args:
            try:
                request_params.start = datetime.strptime(args["start"], DATETIME_FORMAT_REQUEST)
            except ValueError:
                datetime_format_errors.append("start")
        else:
            missing_params.append("start")
        if "end" in args:
            try:
                request_params.end = datetime.strptime(args["end"], DATETIME_FORMAT_REQUEST)
            except ValueError:
                datetime_format_errors.append("end")
        else:
            missing_params.append("end")
        if "device_id" in args:
            request_params.device_id = args["device_id"]
        else:
            missing_params.append("device_id")
        if "content_id" in args:
            request_params.content_id = args["content_id"]
        else:
            missing_params.append("content_id")

        for missing_param in missing_params:
            request_params.add_model_error(MISSING_PARAM_FORAMT.format(missing_param))
        for datetime_format_error in datetime_format_errors:
            request_params.add_model_error(INVALID_DATE_FORMAT.format(datetime_format_error))
        if request_params.start and request_params.end and request_params.start > request_params.end:
            request_params.add_model_error("End time must be later then start time")
        return request_params