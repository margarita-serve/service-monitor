import logging
import os
import threading
from datetime import datetime, timezone

from flask import Flask
from flask_cors import CORS
from flask_restx import Api, Resource, fields, reqparse

import utils
from kafka_func import makeproducer, produceKafka
from metrics import get_servicehealth_metrics, get_servicehealth_timeline, check_status
from resource import get_resource_data

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s]: {} %(levelname)s %(message)s'.format(os.getpid()),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger()

app = Flask(__name__)
CORS(app)
api = Api(app, version='1.0', title='KoreServe Service Monitor Service API')

ns = api.namespace('', describtion='REST-API operations')

#######################################################################
# restX Input Model
#######################################################################

init_data_model = api.model("initData", {
    "inference_name": fields.String(example="mpg-sample", required=True),
    "model_id": fields.String(example="000001", required=True),
})

#######################################################################
# restX Output Model
#######################################################################
base_output_model = api.model("BaseOutputModel", {
    "message": fields.String,
    "inference_name": fields.String,
})

servicehealth_metrics_output_model = api.model("ServicehealthMetricsOutputModel", {
    "message": fields.String,
    "data": fields.String,
    "start_time": fields.String,
    "end_time": fields.String
})


############################################################
# HTTP Routing
############################################################

@ns.route("/servicehealth-monitor/servicehealth/<string:inferencename>")
@ns.param('end_time', 'example=2022-05-13:05', required=True)
@ns.param('start_time', 'example=2022-05-04:05', required=True)
@ns.param('type', 'timeline or aggregation', required=True)
@ns.param('model_history_id', 'Model History ID', required=True)
@ns.param('inferencename', 'Kserve Inferencename')
class ServiceHealthMonitorSearch(Resource):
    @ns.marshal_with(servicehealth_metrics_output_model, code=200, skip_none=True)
    def get(self, inferencename):
        parser = reqparse.RequestParser()
        parser.add_argument('start_time', required=True, type=str,
                            location='args', help='2022-05-04:05')
        parser.add_argument('end_time', required=True, type=str,
                            location='args', help='2022-05-13:05')
        parser.add_argument('type', required=True, type=str,
                            location='args', help='timeline or aggregation')
        parser.add_argument('model_history_id', required=True, type=str,
                            location='args', help='000001')

        args = parser.parse_args()
        inference_name = inferencename

        start_time, end_time, value_type, model_history_id = parsing_servicehealth_data(args)
        try:
            start_time, start_time_str = utils.convertTimestamp(start_time, True)
            end_time, end_time_str = utils.convertTimestamp(end_time, False)
            if start_time > end_time:
                return {"message": "Time not available for lookup."}, 400
            now_time = datetime.now(tz=timezone.utc).replace(tzinfo=None)
            if end_time > now_time:
                cal_hour = True
                offset = now_time.minute
            else:
                cal_hour = False
                offset = 0
        except:
            return {"message": "time parser error, time format must be yyyy-mm-dd:hh"}, 400

        model_id = model_history_id

        if value_type == 'aggregation':
            result, value = get_servicehealth_metrics(inference_name, model_id, start_time_str, end_time_str)
        else:
            result, value = get_servicehealth_timeline(inference_name, model_id, start_time_str, end_time_str, cal_hour,
                                                       offset)

        if result is False:
            if str(value).find("NotFoundError") >= 0:
                return {"message": f'NotFoundError : {value}', "inference_name": inference_name}, 404
            return {"message": f'Error : {value}', "inference_name": inference_name}, 400

        return {"message": "Success get servicehealth metrics", "inference_name": inference_name,
                "start_time": start_time, "end_time": end_time, "data": value}, 200


@ns.route("/servicehealth-monitor")
class ServicehealthMonitorInit(Resource):
    @ns.expect(init_data_model, validate=True)
    @ns.marshal_with(base_output_model, code=201, skip_none=True)
    def post(self):
        args = api.payload
        inference_name = args.get('inference_name')
        model_id = args.get('model_id')

        result, message = create_monitor_setting(inference_name, model_id)

        if result is False:
            return {"message": message, "inference_name": inference_name}, 400

        return {"message": 'ServiceHealth Monitor Inference base data init Success',
                "inference_name": inference_name}, 201


@ns.route("/servicehealth-monitor/disable-monitor/<string:inferencename>")
@ns.param("inferencename", "Kserve Inferencename")
class DisableMonitor(Resource):
    @ns.marshal_with(base_output_model, code=200, skip_none=True)
    def patch(self, inferencename):
        inference_name = inferencename
        result = disable_monitor(inference_name)
        if result is False:
            return {"message": "Disable Failed", "infernecename": inferencename}, 400

        return {"message": "Drift Monitor is disabled", "inference_name": inference_name}, 201


@ns.route("/servicehealth-monitor/enable-monitor/<string:inferencename>")
@ns.param("inferencename", "Kserve Inferencename")
class EnableMonitor(Resource):
    @ns.marshal_with(base_output_model, code=200, skip_none=True)
    def patch(self, inferencename):
        inference_name = inferencename
        result = enable_monitor(inference_name)
        if result is False:
            return {"message": "Enable Failed", "inferencename": inferencename}, 400

        return {"message": "Drift Monitor is enabled", "inference_name": inference_name}, 201


@ns.route("/resource-data/<string:inferencename>")
@ns.param('query', 'Query', required=True)
@ns.param('inferencename', 'Kserve Inferencename')
class ResourceData(Resource):
    def get(self, inferencename):
        inference_name = inferencename
        parser = reqparse.RequestParser()
        parser.add_argument('query', required=True, type=str,
                            location='args', help='query')
        args = parser.parse_args()

        query = args.get("query")
        result = get_resource_data(query)
        if result is False:
            return False, 400
        return result


############################################################
# Domain Logic
############################################################

def parsing_servicehealth_data(request_args):
    start_time = request_args.get('start_time')
    end_time = request_args.get('end_time')
    value_type = request_args.get('type')
    model_history_id = request_args.get('model_history_id')

    return start_time, end_time, value_type, model_history_id


def create_monitor_setting(inference_name, model_id):
    setting = {
        "current_model": model_id,
        "status": "enable"
    }
    result, message = utils.save_data('servicehealth_monitor_setting', inference_name, setting)
    if result is False:
        return False, message
    else:
        return True, message


def servicehealthMonitor():
    query = {
        "match": {
            "status": "enable"
        }
    }
    result, items = utils.search_index("servicehealth_monitor_setting", query)
    if result is False:
        logger.warning(items)
    else:
        producer = makeproducer()
        if producer is False:
            logger.warning('kafka.errors.NoBrokersAvailable: NoBrokersAvailable')
        else:
            for item in items:
                inference_name = item['_id']
                model_id = item['_source']['current_model']
                try:
                    servicehealth_result = check_status(inference_name, model_id)
                    produceKafka(producer, {"inference_name": inference_name, "result": servicehealth_result})
                except Exception as err:
                    logger.warning(err)

            producer.close()
    threading.Timer(60, servicehealthMonitor).start()


def disable_monitor(inference_name):
    result, message = utils.update_data('servicehealth_monitor_setting', inference_name, {"status": "disable"})
    if result is False:
        return False
    return True


def enable_monitor(inference_name):
    result, message = utils.update_data('servicehealth_monitor_setting', inference_name, {"status": "enable"})
    if result is False:
        return False
    return True


############################################################
# Main
############################################################


if __name__ == '__main__':
    servicehealthMonitor()

    # test
    # app.run(threaded=True, port=8004)
