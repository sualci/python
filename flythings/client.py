#!/usr/bin/python
# -*- coding: utf-8 -*-
import ast
import base64
import copy
import json
import os
import socket
import sys
import time
from datetime import timedelta, datetime
from enum import Enum
from inspect import signature
from threading import Thread, Event, Lock
from urllib.parse import urlparse
from flythings.config import ServerConfig
import requests

from flythings.paths import FILE, LOGIN_USER_URL, LOGIN_DEVICE_URL, HTTP_, HTTPS_, FOI_URL, PUBLISH_MULTIPLE_URL, \
    PUBLISH_PREDICTION_MULTIPLE_URL, PUBLISH_RECORD_URL, PUBLISH_PLAIN_CSV_URL, GET_OBSERVATIONS_URL, \
    GET_LAST_VALUE_URL, GET_PREDICTIONS_URL, PUBLISH_SINGLE_URL, PUBLISH_PREDICTION_SINGLE_URL, SERIES_URL, \
    DEVICE_METADATA_URL, PUBLISH_INFRASTRUCTURE_METADATA, PUBLISH_INFRASTRUCTURE_SIMPLE, PUBLISH_INFRASTRUCTURE, \
    SOCKET_URL, ACTIONS_URL, DEVICE_ALERT_URL


class ActionDataTypes(Enum):
    BOOLEAN = 'BOOLEAN'
    NUMBER = 'NUMBER'
    TEXT = 'TEXT'
    DATE = 'DATE'
    SELECTOR = 'SELECTOR'
    ARRAY = 'ARRAY'
    JSON = 'JSON'
    FILE = 'FILE'
    LIVE = 'LIVE'


class SamplingFeatureType(Enum):
    POINT = {
        'id': 1,
        'type': 'http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingPoint'
    }
    LINE = {
        'id': 2,
        'type': 'http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingSurface'
    }
    POLYGON = {
        'id': 3,
        'type': 'http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_Specimen'
    }
    NO_POSITION = {
        'id': 4,
        'type': 'http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingCurve'
    }


class ServerClient:
    thread = None
    lock = None
    
    headers = {'x-auth-token': '', 'Content-Type': 'application/json'}

    real_time_acumulator = {}
    batch_enabled = False
    batch_timeout = 50
    real_time_timeout = 1400
    last_real_time_timestamp = None
    action_socket = None

    client_tcp_socket = None
    client_udp_socket = None
    client_action_thread = None
    callbacks = {}
    action_thread_stop = False

    def __init__(self, config: ServerConfig):
        self.config = config
        self.thread = Thread(target=self.__send_socket_batch)
        self.lock = Lock()
        self.set_authorization_token(config.authorization)
        self.set_token(config.token)
        self.set_server(config.server)

    def login(self, user, password, login_type):
        try:
            if login_type == 'DEVICE':
                authbody = requests.get(self.config.server + LOGIN_DEVICE_URL, auth=(user, password),
                                        timeout=self.config.timeout)
            elif login_type == 'USER':
                authbody = requests.get(self.config.server + LOGIN_USER_URL, auth=(user, password),
                                        timeout=self.config.timeout)
            else:
                login_type = 'USER'
                authbody = requests.get(self.config.server + LOGIN_USER_URL, auth=(user, password),
                                        timeout=self.config.timeout)
            if authbody.status_code == 200:
                body = json.loads(authbody.text)
                self.headers['x-auth-token'] = str(body['token'])
                if login_type == 'USER' and 'workspace' in body:
                    self.headers['Workspace'] = str(body['workspace'])
                else:
                    self.headers['Workspace'] = str(self.config.workspace)
                return str(body['token'])
            else:
                print('ERROR AUTHENTICATED, CHECK THE USER OR PASSWORD')
                return None
        except requests.exceptions.InvalidURL:
            print('INVALID SERVER')
            raise

    def logout(self):
        self.headers.pop('x-auth-token', None)
        self.headers['x-auth-token'] = ''
        self.headers.pop('Workspace', None)
        self.headers.pop('Authorization', None)

    def load_data_by_file(self, file=None):
        if file is None:
            file = FILE
        try:
            for line in open(file):
                text = line.strip().replace('\n', '')
                list_param = text.split(':')
                self.__update_file_params(list_param)
            if self.config.user != '' and self.config.password != '':
                self.login(self.config.user, self.config.password, self.config.login_type)
            if self.config.server == '':
                self.config.server = 'api.flythings.io'
            print('Succesfully loaded data from file ' + file)
        except Exception:
            print('CONFIGURATION FILE, ' + file + ' DONT EXIST, YOU MUST INSERT THE PARAMETERS MANUALLY')

    def __update_file_params(self, list_param):
        if len(list_param) > 1:
            if list_param[0].lower() == 'token':
                list_param[1] = list_param[1].strip()
                self.config.token = list_param[1]
                self.headers['x-auth-token'] = list_param[1]
            elif list_param[0].lower() == 'server':
                list_param[1] = list_param[1].strip()
                self.config.server = list_param[1]
            elif list_param[0].lower() == 'user':
                list_param[1] = list_param[1].strip()
                self.config.user = list_param[1]
            elif list_param[0].lower() == 'password':
                list_param[1] = list_param[1].strip()
                self.config.password = list_param[1]
            elif list_param[0].lower() == 'login_type':
                list_param[1] = list_param[1].strip()
                self.config.login_type = list_param[1]
            elif list_param[0].lower() == 'hash':
                list_param[1] = list_param[1].strip()
                self.config.hash = list_param[1]
            elif list_param[0].lower() == 'device':
                list_param[1] = list_param[1].strip()
                self.config.foi = list_param[1]
            elif list_param[0].lower() == 'sensor':
                list_param[1] = list_param[1].strip()
                self.config.procedure = list_param[1]
            elif list_param[0].lower() == 'timeout':
                list_param[1] = list_param[1].strip()
                self.config.timeout = list_param[1]
            elif list_param[0].lower() == 'authorization':
                self.headers['Authorization'] = "Bearer " + list_param[1]
                self.headers['x-auth-token'] = '-'

    def set_server(self, server):
        if server is not None:
            server = server.strip()
            if server.endswith('/'):
                server = server[:-1]
            if not server.startswith(HTTP_) and not server.startswith(HTTPS_):
                server = HTTP_ + server
            self.config.server = server
        return self.config.server

    def get_server(self):
        return self.config.server

    def __update_foi_file(self):
        file = open(".foiCache", "r")
        for line in file:
            line_items = line.split('\t')
            if line_items[0] == self.config.server and line_items[1] == self.config.foi:
                return False
        file.close()
        file = open('.foiCache', 'a')
        file.write(self.config.server + '\t' + self.config.foi + '\t' + '\n')
        file.close()
        return True

    def set_device(self, device, object=None, always_update=False):
        self.config.foi = device
        foi_to_send = {'featureOfInterest': {"name": device}}
        if object is not None:
            if 'type' in object:
                response = requests.get(self.config.server + FOI_URL + '/devicetypes', headers=self.headers, timeout=self.config.timeout)
                if response.status_code == 200:
                    device_types = response.json()
                    if object['type'] in device_types:
                        foi_to_send['device'] = object['type']
                else:
                    print(str(response.status_code) + "FAIL RETRIEVING DEVICE TYPES")
            if 'geom' in object:
                foi_to_send['featureOfInterest']['geom'] = object['geom']
        if self.__update_foi_file() or always_update:
            requests.post(self.config.server + FOI_URL, json.dumps(foi_to_send), headers=self.headers,
                          timeout=self.config.timeout)
        return self.config.foi

    def set_custom_header(self, header, header_value):
        self.headers[header] = header_value
        return self.headers[header]

    def get_headers(self):
        return self.headers

    def set_sensor(self, sensor):
        self.config.procedure = sensor
        return self.config.procedure

    def set_token(self,token):
        self.headers['x-auth-token'] = token
        return self.headers['x-auth-token']

    def set_authorization_token(self, token):
        self.headers['Authorization'] = "Bearer " + token
        self.headers['x-auth-token'] = '-'
        return self.headers['Authorization']

    def set_workspace(self, workspace):
        self.config.workspace = workspace
        return self.config.workspace

    def set_timeout(self, timeout):
        self.config.timeout = timeout
        return self.config.timeout

    def set_batch_enabled(self, batch_enabled):
        self.batch_enabled = batch_enabled
        if self.batch_enabled :
            if not self.thread.is_alive():
                self.thread.start()
        else:
            if self.thread.is_alive():
                self.thread.join()
        return self.batch_enabled

    def send_observations(self, values):
        response = None
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return response
        if values is None:
            print('Values cannot be None')
            return response
        if len(values) > 1000:
            i = 0
            while i < len(values):
                aux_values = values[i:i + 1000]
                response = self.__send_observations(aux_values)
                if response >= 400:
                    return response
                i += 1000
        else:
            response = self.__send_observations(values)
        return response

    def __send_observations(self,values):
        response = None
        try:
            response = requests.put(self.config.server + PUBLISH_MULTIPLE_URL, data=json.dumps({'observations': values}),
                                    headers=self.headers, timeout=self.config.timeout)
        except Exception as e:
            print(e)
        if response is not None:
            if response.status_code >= 400:
                print(response.text)
            return response.status_code
        else:
            print("NO RESPONSE FROM SERVICE")
            return 502

    def send_predictions(self, values):
        response = None
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return response
        if values is None:
            print('Values cannot be None')
            return response
        if len(values) > 1000:
            i = 0
            while i < len(values):
                aux_values = values[i:i + 1000]
                response = self.__send_predictions(aux_values)
                if response >= 400:
                    return response
                i += 1000
        else:
            response = self.__send_predictions(values)
        return response

    def __send_predictions(self, values):
        response = None
        try:
            response = requests.put(self.config.server + PUBLISH_PREDICTION_MULTIPLE_URL,
                                    data=json.dumps({'predictions': values}),
                                    headers=self.headers, timeout=self.config.timeout)
        except Exception as e:
            print(e)
        if response is not None:
            if response.status_code >= 400:
                print(response.text)
            return response.status_code
        else:
            print("NO RESPONSE FROM SERVICE")
            return 502

    def send_record(self, serie_id, observations):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None
        if isinstance(observations, str):
            response = requests.put(self.config.server + PUBLISH_RECORD_URL + "/" + str(serie_id), observations,
                                    headers=self.headers)
        else:
            response = requests.put(self.config.server + PUBLISH_RECORD_URL + "/" + str(serie_id),
                                    data=json.dumps(observations), headers=self.headers)
        return response.status_code, response.content

    def send_observations_csv(self, values):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None
        response = requests.post(self.config.server + PUBLISH_PLAIN_CSV_URL, data=values, headers=self.headers,
                                 timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
        return response.status_code, response.content

    def search(
        self,
        series,
        start_date=None,
        end_date=None,
        aggrupation=None,
        aggrupation_type=None,
        as_incremental=False
    ):
        if self.headers['x-auth-token'] == '':
            return 'NoAuthenticationError'

        # Default datetime
        if start_date is None and end_date is None:
            end_date = round(time.time() * 1000)
            aux_time = datetime.today() - timedelta(weeks=1)
            start_date = round(aux_time.timestamp() * 1000)
        elif end_date is None:
            end_date = round(time.time() * 1000)

        message = {'series': [{'id': series, 'asIncremental': as_incremental}], 'startDate': start_date,
                   'endDate': end_date}

        if aggrupation is not None:
            message['temporalScale'] = aggrupation
        if aggrupation_type is not None:
            message['temporalScaleType'] = aggrupation_type
        r = requests.post(self.config.server + GET_OBSERVATIONS_URL, data=json.dumps(message), headers=self.headers,
                          timeout=self.config.timeout)
        if r.status_code == 200:
            list = r.json()[0]['data']
            return_list = []
            for elem in list:
                return_list.append({'value': elem[1], 'time': elem[0]})
            return return_list
        else:
            print(r.text)

    def get_last_observation_before_date(self, series_id, timestamp):
        if self.headers['x-auth-token'] == '':
            return 'NoAuthenticationError'

        r = requests.get(self.config.server + GET_LAST_VALUE_URL + f"/{series_id}/{timestamp}", headers=self.headers,
                         timeout=self.config.timeout)

        if r.status_code != 200 or len(r.content) == 0:
            print(r.text)
            return None

        result = r.json()
        return result['time'], result['value']

    def search_prediction(
        self,
        series,
        start_date=None,
        end_date=None,
        aggrupation=None,
        aggrupation_type=None,
        as_incremental=False
    ):
        if self.headers['x-auth-token'] == '':
            return 'NoAuthenticationError'

        # Default datetime
        if start_date is None and end_date is None:
            end_date = round(time.time() * 1000)
            aux_time = datetime.today() - timedelta(weeks=1)
            start_date = round(aux_time.timestamp() * 1000)
        elif end_date is None:
            end_date = round(time.time() * 1000)

        message = {'series': [{'id': series, 'asIncremental': as_incremental}], 'startDate': start_date,
                   'endDate': end_date}

        if aggrupation is not None:
            message['temporalScale'] = aggrupation
        if aggrupation_type is not None:
            message['temporalScaleType'] = aggrupation_type
        r = requests.post(self.config.server + GET_PREDICTIONS_URL, data=json.dumps(message), headers=self.headers,
                          timeout=self.config.timeout)
        if r.status_code == 200:
            list = r.json()[0]['data']
            return_list = []
            for elem in list:
                return_list.append({'value': elem[1], 'time': elem[0]})
            return return_list
        else:
            print(r.text)

    def send_observation(
        self,
        value,
        property,
        uom=None,
        ts=None,
        geom=None,
        procedure=None,
        foi=None,
        device_type=None,
        foi_name=None
    ):

        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None
        message = self.get_observation(value, property, uom, ts, geom, procedure, foi, device_type, foi_name)
        json_payload = json.dumps(message)
        response = requests.put(self.config.server + PUBLISH_SINGLE_URL, json_payload, headers=self.headers, timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
        return response.status_code

    def send_prediction(
        self,
        value,
        property,
        uom=None,
        ts=None,
        geom=None,
        procedure=None,
        foi=None,
        device_type=None,
        foi_name=None
    ):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None
        message = self.get_observation(value, property, uom, ts, geom, procedure, foi, device_type, foi_name)
        json_payload = json.dumps(message)
        response = requests.put(self.config.server + PUBLISH_PREDICTION_SINGLE_URL, json_payload, headers=self.headers,
                                timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
        return response.status_code

    def get_image_observation(
        self,
        file,
        property,
        format=None,
        uom=None,
        ts=None,
        geom=None,
        procedure=None,
        foi=None,
        device_type=None,
        foi_name=None
    ):
        if isinstance(file, str):
            with open(file, "rb") as img_file:
                b64_string = base64.b64encode(img_file.read())
                f = b64_string.decode('utf-8')
            if file.rsplit(".") is not None and len(file.rsplit(".")) > 1:
                format = file.rsplit(".")[1]
        else:
            f = base64.b64encode(file.read()).decode('utf-8')
        if format is None:
            print('Format cannot be None')
            return None
        message = {'observableProperty': property, "file": {"file": f, "format": format}}
        if uom is not None:
            message['uom'] = uom
        if ts is not None:
            message['time'] = ts
        if geom is not None:
            message['geom'] = geom
        if procedure is not None and procedure != '':
            message['procedure'] = procedure
        else:
            message['procedure'] = self.config.procedure
        if foi is not None and foi != '':
            message['foi'] = foi
        else:
            message['foi'] = self.config.foi
        if device_type is not None:
            message['deviceType'] = device_type
        if foi_name is not None:
            message['foiName'] = foi_name
        return message

    def get_image_bytes_observation(
        self,
        bytes,
        property,
        format,
        uom=None,
        ts=None,
        geom=None,
        procedure=None,
        foi=None,
        device_type=None,
        foi_name=None
    ):
        f = base64.b64encode(bytes).decode('utf-8')
        message = {'observableProperty': property, "file": {"file": f, "format": format}}
        if uom is not None:
            message['uom'] = uom
        if ts is not None:
            message['time'] = ts
        if geom is not None:
            message['geom'] = geom
        if procedure is not None and procedure != '':
            message['procedure'] = procedure
        else:
            message['procedure'] = self.config.procedure
        if foi is not None and foi != '':
            message['foi'] = foi
        else:
            message['foi'] = self.config.foi
        if device_type is not None:
            message['deviceType'] = device_type
        if foi_name is not None:
            message['foiName'] = foi_name
        return message

    def get_image_base64_observation(
        self,
        base_64,
        property,
        format,
        uom=None,
        ts=None,
        geom=None,
        procedure=None,
        foi=None,
        device_type=None,
        foi_name=None
    ):
        f = base_64.decode('utf-8')
        message = {'observableProperty': property, "file": {"file": f, "format": format}}
        if uom is not None:
            message['uom'] = uom
        if ts is not None:
            message['time'] = ts
        if geom is not None:
            message['geom'] = geom
        if procedure is not None and procedure != '':
            message['procedure'] = procedure
        else:
            message['procedure'] = self.config.procedure
        if foi is not None and foi != '':
            message['foi'] = foi
        else:
            message['foi'] = self.config.foi
        if device_type is not None:
            message['deviceType'] = device_type
        if foi_name is not None:
            message['foiName'] = foi_name
        return message

    def get_observation(
        self,
        value,
        property,
        uom=None,
        ts=None,
        geom=None,
        procedure=None,
        foi=None,
        device_type=None,
        foi_name=None,
        force_type=None
    ):
        message = {'observableProperty': property, 'value': value}
        if uom is not None:
            message['uom'] = uom
        if ts is not None:
            message['time'] = ts
        if geom is not None:
            message['geom'] = geom
        if procedure is not None and procedure != '':
            message['procedure'] = procedure
        else:
            message['procedure'] = self.config.procedure
        if foi is not None and foi != '':
            message['foi'] = foi
        else:
            message['foi'] = self.config.foi
        if device_type is not None:
            message['deviceType'] = device_type
        if foi_name is not None:
            message['foiName'] = foi_name
        if force_type is not None:
            message['forceType'] = force_type
        return message

    def get_observation_csv(
        self,
        value,
        series=None,
        uom=None,
        ts=None,
        property=None,
        procedure=None,
        foi=None,
    ):
        message = ''
        if series is not None:
            message += str(series) + ";"
        else:
            if foi is not None:
                message += foi + ";"
            else:
                message += self.config.foi + ";"
            if procedure is not None:
                message += procedure + ";"
            else:
                message += self.config.procedure + ";"
            if property is not None:
                message += property + ";"
            else:
                return None
        if ts is not None:
            message += str(ts)
        else:
            message += str(int(time.time() * 1000))
        message += ";" + value
        if uom is not None:
            message += ";" + uom
        return message

    def find_series(self, foi=None, procedure=None, observable_property=None):
        if foi is None or foi == '':
            foi = self.config.foi
        if procedure is None or procedure == '':
            procedure = self.config.procedure
        if observable_property is None or observable_property == '':
            return "INSERT A OBSERVABLE PROPERTY"
        response = requests.get(self.config.server + SERIES_URL + foi + '/' + procedure + '/' + observable_property,
                                headers=self.headers, timeout=self.config.timeout)
        if response.status_code != 200:
            print("Error retrieving series: " + foi + "-" + procedure + "-" + observable_property)
            return None
        try:
            message = json.loads(response.content.decode("utf-8"))
            return message
        except:
            return None

    def save_text_metadata(self, key, value, foi=None):
        if foi is None or foi == '':
            foi = self.config.foi
        message = {'key': key.upper(), 'value': value, 'type': 'TEXT'}
        json_payload = json.dumps(message)
        response = requests.put(self.config.server + DEVICE_METADATA_URL + '/identifier/' + foi, json_payload,
                                headers=self.headers, timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
        return response.status_code

    def save_date_metadata(self,key, value, foi=None):
        if foi is None or foi == '':
            foi = self.config.foi
        message = {'key': key.upper(), 'value': value, 'type': 'DATE'}
        json_payload = json.dumps(message)
        response = requests.put(self.config.server + DEVICE_METADATA_URL + '/identifier/' + foi, json_payload,
                                headers=self.headers, timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
        return response.status_code

    @staticmethod
    def get_text_metadata(key, value, tag_id=None):
        metadata = {'key': key.upper(), 'value': value, 'tagId': tag_id, 'type': 'TEXT'}
        return metadata

    @staticmethod
    def get_infrastructure(
        name,
        type,
        geom=None,
        geom_type=None,
        fois=None,
        alias=None
    ):
        infrastructure = {'name': name, 'type': type}
        if alias is not None:
            infrastructure['alias'] = alias
        if geom is not None:
            infrastructure['geom'] = geom
        if geom_type is not None and geom_type.value is not None:
            infrastructure['geomType'] = geom_type.value
        else:
            infrastructure['geomType'] = SamplingFeatureType.NO_POSITION.value
        if fois is not None and fois:
            infrastructure['featureOfInterestList'] = fois
        return infrastructure

    @staticmethod
    def get_infrastructure_withmetadata(
        name,
        type,
        geom=None,
        geom_type=None,
        fois=None,
        text_metadata_list=None,
        alias=None
    ):
        infrastructure = {'name': name, 'type': type}
        if alias is not None:
            infrastructure['alias'] = alias
        if geom is not None:
            infrastructure['geom'] = geom
        if geom_type is not None and geom_type.value is not None:
            infrastructure['geomType'] = geom_type.value
        else:
            infrastructure['geomType'] = SamplingFeatureType.NO_POSITION.value
        if fois is not None and fois:
            infrastructure['featureOfInterestList'] = fois
        if text_metadata_list is not None and text_metadata_list != '':
            infrastructure['textMetadata'] = text_metadata_list
        return infrastructure

    def save_infrastructure(self, infrastructure, id=None):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None, None
        if id is not None:
            infrastructure.id = id
        json_payload = json.dumps(infrastructure)
        response = requests.post(self.config.server + PUBLISH_INFRASTRUCTURE_METADATA,
                                 json_payload, headers=self.headers, timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
            return response.status_code, None
        return response.status_code, json.loads(response.text)

    def save_infrastructure_with_metadata(self, infrastructure, id=None):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None, None
        if id is not None:
            infrastructure.id = id
        json_payload = json.dumps(infrastructure)
        response = requests.post(self.config.server + PUBLISH_INFRASTRUCTURE_METADATA,
                                 json_payload, headers=self.headers, timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
            return response.status_code, None
        return response.status_code, json.loads(response.text)

    def save_infrastructure_without_override_fois(self, infrastructure, id=None):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None, None
        if id is not None:
            infrastructure.id = id
        json_payload = json.dumps(infrastructure)
        response = requests.post(self.config.server + PUBLISH_INFRASTRUCTURE_SIMPLE,
                                 json_payload, headers=self.headers, timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
            return response.status_code, None
        return response.status_code, json.loads(response.text)

    def link_device_to_infrastructure(self,infrastructure_tree, foi_identifier):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None
        if foi_identifier is None:
            print('foi_identifier is None')
            return None
        json_payload = json.dumps(infrastructure_tree)
        response = requests.put(
            self.config.server + PUBLISH_INFRASTRUCTURE + "/link/featureofinterest/" + foi_identifier,
            json_payload, headers=self.headers, timeout=self.config.timeout)
        if response.status_code >= 400:
            print(response.text)
        return response.status_code

    def __get_tcp_socket(self,url=None):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None

        decode = lambda d: d.decode('utf-8')
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            response = requests.get(self.config.server + (SOCKET_URL if url is None else url), headers=self.headers)
            if response.status_code == 200:
                port = int(response.text)
                url = urlparse(self.config.server)
                s.connect((url.hostname, port))

                data = s.recv(1024)

                if decode(data) == 'X-AUTH-TOKEN':
                    if 'Authorization' in self.headers and self.headers['Authorization'] is not None \
                        and self.headers['x-auth-token'] == '-':
                        s.sendall((self.headers['Authorization'] + "\n").encode("utf-8"))
                    else:
                        s.sendall((self.headers['x-auth-token'] + "\n").encode("utf-8"))
                    is_logged = s.recv(4)
                    if decode(is_logged) == 'True':
                        return s
                    else:
                        print("INVALID_TOKEN")
                        s.close()
                else:
                    print("SOCKET UNAVAILABLE!")
                    s.close()
        except:
            print("Connection refused")
        return None

    def __get_udp_socket(self):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if ":" not in self.config.server:
                if "/" not in self.config.server:
                    server = self.config.server
                else:
                    server = self.config.server.split("/")[0]
            else:
                server = self.config.server.split(":")[0]

            response = requests.get(self.config.server + SOCKET_URL, headers=self.headers)
            if response.status_code == 200:
                port = int(response.text)

                s.connect((server, port))
                return s
        except:
            print("Connection refused")
        return None

    def __get_socket(self, protocol):
        if protocol is None or protocol.upper() == "TCP":
            if self.client_tcp_socket is None:
                self.client_tcp_socket = self.__get_tcp_socket()
                if self.client_tcp_socket is not None:
                    self.client_tcp_socket.settimeout(15.0)
            return self.client_tcp_socket
        else:
            if self.client_udp_socket is None:
                self.client_udp_socket = self.__get_udp_socket()
            return self.client_udp_socket

    def __get_payload(self, series_id, value, timestamp, protocol):
        if protocol is None or protocol.upper() == "TCP":
            return json.dumps(
                {
                    'seriesId': series_id,
                    'obs': [{
                        'seriesId': series_id,
                        'timestamp': timestamp,
                        'value': value
                    }]
                }) + "\n"
        else:
            return json.dumps({
                'X-AUTH-TOKEN': self.headers['x-auth-token'],
                'data': {

                    'seriesId': series_id,
                    'obs': [{
                        'seriesId': series_id,
                        'timestamp': timestamp,
                        'value': value
                    }]
                }
            })

    def __reset_socket(self,protocol):
        if protocol is None or protocol.upper() == "TCP":
            if self.config.client_tcp_socket is not None:
                self.config.client_tcp_socket.close()
                self.config.client_tcp_socket = None
        else:
            if self.config.client_udp_socket is not None:
                self.config.client_udp_socket.close()
                self.config.client_udp_socket = None

    def send_socket(self,series_id, value, timestamp, protocol=None):
        if self.batch_enabled:
            self.__save_batch_socket(series_id, value, timestamp)
        else:
            self.__send_socket(series_id, value, timestamp, protocol)

    def __save_batch_socket(self, series_id, value, timestamp):
        self.lock.acquire()
        if str(series_id) in self.real_time_acumulator:
            if int(time.time() * 1000) - \
                self.real_time_acumulator[str(series_id)][len(self.real_time_acumulator[str(series_id)]) - 1][
                    'timestamp'] >= self.batch_timeout:
                self.real_time_acumulator[str(series_id)].append({
                    'seriesId': series_id,
                    'timestamp': timestamp,
                    'value': value
                })
            else:
                self.lock.release()
                e_message = 'ERROR, DEVICE MUST WAIT AT LEAST 50ms BEFORE ACUMULATE ANOTHER OBSERVATION'
                self.__print__(e_message)
                return e_message
        else:
            self.real_time_acumulator[str(series_id)] = [{
                'seriesId': series_id,
                'timestamp': timestamp,
                'value': value
            }]
        self.lock.release()

    def __send_socket(self, series_id, value, timestamp, protocol=None):
        if self.last_real_time_timestamp is None or int(
            time.time() * 1000) - self.last_real_time_timestamp >= self.real_time_timeout:
            client_socket = self.__get_socket(protocol)

            if client_socket is not None:
                json_payload = self.__get_payload(series_id, value, timestamp, protocol)
                try:
                    client_socket.sendall(json_payload.encode("utf-8"))
                    try:
                        client_socket.recv(1024)
                    except socket.timeout:
                        print("Buffer was already empty")
                except socket.error as msg:
                    self.__reset_socket(protocol)
                    print(msg)
            else:
                print("ERROR CONNECTING TO SOCKET")
            print('CORRECT SENDED')
            self.last_real_time_timestamp = int(time.time() * 1000)
        else:
            e_message = 'ERROR, DEVICE MUST WAIT AT LEAST 1400ms BEFORE SEND A OBSERVATION FROM REALTIME'
            print(e_message)
            return e_message

    def __acumulator_series_to_json(data):
        return json.dumps({'seriesId': data[0]['seriesId'],
                           'obs': sorted(data, key=lambda o: o['timestamp'])
                           }) + "\n"

    def __send_socket_batch(self,protocol=None):
        while True:
            if self.batch_enabled:
                client_socket = self.__get_socket(protocol)
                self.lock.acquire()
                acumulator = copy.deepcopy(self.config.real_time_acumulator)
                if client_socket is not None:
                    self.config.real_time_acumulator = {}
                    self.lock.release()
                    try:
                        values = acumulator.values()
                        for value in values:
                            json_payload = self.__acumulator_series_to_json(value)
                            client_socket.sendall(json_payload.encode("utf-8"))
                            try:
                                client_socket.recv(1024)
                            except socket.timeout:
                                print("Buffer was already empty")
                    except (socket.error, socket.timeout) as msg:
                        self.__print__("SOCKET ERROR:" + str(msg))
                        self.__reset_socket(protocol)
                else:
                    self.lock.release()
                    self.__print__("ERROR CONNECTING TO SOCKET")
            time.sleep(5)

    def __register_action(
        self,
        name,
        parameter_type=None,
        foi=None,
        procedure=None,
        observable_property=None,
        unit=None,
        alias=None,
        action_options=None,
        json_template=None
    ):
        if self.headers['x-auth-token'] == '':
            print('NoAuthenticationError')
            return None
        elif foi is None and self.config.foi is None:
            print('NoDeviceError')
            return None
        elif observable_property is not None and (self.config.procedure is None and procedure is None):
            print('NoProcedureError')
            return None
        try:
            payload = {
                "name": name,
                "featureOfInterest": self.config.foi if foi is None else foi,
                "parameterType": ActionDataTypes(parameter_type).name if parameter_type is not None else None
            }
            if observable_property is not None:
                payload["procedure"] = self.config.procedure if procedure is None else procedure
                payload["observableProperty"] = observable_property
            if unit is not None:
                payload["unit"] = unit
            if alias is not None:
                payload["alias"] = alias
            if action_options is not None and parameter_type.name == ActionDataTypes.SELECTOR.name:
                payload["actionOptions"] = action_options
            if json_template is not None:
                payload["jsonTemplate"] = json_template
            response = requests.post(self.config.server + ACTIONS_URL, data=json.dumps(payload), headers=self.headers)

            if response.status_code == 201:
                return True
            else:
                print(response.status_code)
        except Exception as e:
            print(e)
        return None

    def register_action_for_series(
        self,
        name,
        observable_property,
        unit,
        callback,
        foi=None,
        procedure=None,
        parameter_type=None,
        alias=None,
        action_options=None,
        json_template=None
    ):
        result = self.__register_action(name, parameter_type, foi, procedure, observable_property, unit, alias=alias,
                                   action_options=action_options, json_template=json_template)
        if result:

            if name not in self.callbacks:
                self.callbacks[name] = {'callback': callback, 'parameterType': parameter_type}
            else:
                return False
        return result is not None

    def register_action(self, name, callback, foi=None, parameter_type=None, alias=None, action_options=None,
                        json_template=None):
        result = self.__register_action(name, parameter_type, foi, alias=alias, action_options=action_options,
                                   json_template=json_template)
        if result:
            if name not in self.callbacks:
                self.callbacks[name] = {'callback': callback, 'parameterType': parameter_type}
            else:
                return False
        return result is not None

    def __action_socket_client(self, action_thread_stop, callbacks, foi):
        current_time = time.time()
        self.config.action_socket = None
        while not action_thread_stop.is_set():
            action_time = time.time()
            try:
                if self.config.action_socket is None:
                    self.config.action_socket = self.__get_tcp_socket(ACTIONS_URL)
                if self.config.action_socket is not None:
                    self.config.action_socket.settimeout(60.0)
                    data = self.config.action_socket.recv(1024)
                    decoded_data = data.decode("utf-8")
                    if decoded_data != '':
                        self.__parse_decoded_data(decoded_data, self.config.action_socket, foi)
                    try:
                        if action_time - current_time > 5:
                            self.config.action_socket.sendall("Ping\n".encode("utf-8"))
                            current_time = time.time()
                    except:
                        print("The server closed the connection!")
                        self.config.action_socket.close()
                        self.config.action_socket = None
                else:
                    print("Could not open Action socket, waiting 30 secs to retry")
                    time.sleep(30)
            except socket.timeout:
                print("timeout")
                self.config.action_socket.close()
                self.config.action_socket = None
                time.sleep(30)
                # __action_socket_client(actionThreadStop, callbacks, foi)
            except Exception as e:
                print(str(e))
                if str(e) != "'@PING@'":
                    print("INTERNAL_FAILURE")
                    if self.config.action_socket is not None:
                        self.config.action_socket.close()
                        self.config.action_socket = None
                    time.sleep(30)
                    # __action_socket_client(actionThreadStop, callbacks, foi)
        self.config.action_socket.close()

    def __parse_decoded_data(self, decoded_data, action_socket, foi):
        if decoded_data == "DEVICE":
            action_socket.sendall((foi + "\n").encode("utf-8"))
        else:
            if '@PING@' not in decoded_data:
                response = json.loads(decoded_data)
                param = None
                ts = response["timestamp"]
                command = response["name"]
                action_log = response["actionLog"]
                if 'action' in response:
                    param = response["action"]
                # if callbacks[command] is not None:
                if command in self.callbacks:
                    try:
                        sig = signature(self.callbacks[command]['callback'])
                        if len(sig.parameters) == 0:
                            result = self.callbacks[command]['callback']()
                        elif len(sig.parameters) == 1:
                            result = self.callbacks[command]['callback'](
                                self.__cast_parameter(param, self.callbacks[command]['parameterType']))
                        elif len(sig.parameters) == 2:
                            result = self.callbacks[command]['callback'](
                                self.__cast_parameter(param, self.callbacks[command]['parameterType']), ts)
                        else:
                            result = self.callbacks[command]['callback'](
                                self.__cast_parameter(param, self.callbacks[command]['parameterType']), ts, action_log)
                    except Exception as e:
                        print(e)
                        print("ERROR DOING ACTION")
                        result = e
                    try:
                        if result == 0 or isinstance(result, str):
                            action_socket.sendall((str(result).replace('\n', '') + '\n').encode("utf-8"))
                        else:
                            action_socket.sendall("\n".encode("utf-8"))
                    except Exception as e:
                        print(e)
                        print("ERROR SENDING RESPONSE")

    def __cast_parameter(param, parameter_type):
        text_actions = [ActionDataTypes.TEXT, ActionDataTypes.FILE, ActionDataTypes.SELECTOR, ActionDataTypes.LIVE,
                        ActionDataTypes.JSON, ActionDataTypes.DATE]
        try:
            if parameter_type is None:
                return None
            elif parameter_type in text_actions:
                return param
            elif parameter_type == ActionDataTypes.ARRAY:
                return param.split(";")
            elif parameter_type == ActionDataTypes.BOOLEAN:
                return param.lower() == 'true'
            elif parameter_type == ActionDataTypes.NUMBER:
                return ast.literal_eval(param)  # Number
        except:
            return None

    def send_progress_action(self, message):
        try:
            if self.config.action_socket is None:
                raise Exception("Action Socket is not available")
            if message == 0 or isinstance(message, str):
                self.action_socket.sendall((str(message).replace('\n', '') + '\n').encode("utf-8"))
            else:
                self.action_socket.sendall("\n".encode("utf-8"))
        except Exception as e:
            print(e)
            print("ERROR SENDING PROGRESS ACTION")

    def start_action_listening(self, foi=None):
        if foi is not None and foi != '':
            f = foi
        else:
            f = self.config.foi
        if f is None or f == '':
            print("NoDeviceException")
            return None
        if not self.callbacks:
            print("NoRegisteredActionExcetion")
            return None
        self.action_thread_stop = Event()
        self.client_action_thread = Thread(target=self.__action_socket_client, args=(self.action_thread_stop, self.callbacks, f))
        self.client_action_thread.start()

    def stop_action_listening(self):
        if self.action_thread_stop:
            self.action_thread_stop.set()
        self.config.client_action_thread = None

    def api_get_request(self, url):
        response = requests.get(self.config.server + url, headers=self.headers, timeout=self.config.timeout)
        if response is not None:
            if response.status_code >= 400:
                response.status_code, response.text
        else:
            print("NO RESPONSE FROM SERVICE")
            return 502
        return response.status_code, json.loads(response.text)

    def send_alert(self, subject, text):
        response = requests.put(self.config.server + DEVICE_ALERT_URL, data=json.dumps({
            "subject": subject,
            "text": text
        }), headers=self.headers)
        return response.status_code, json.loads(response.text)

    def __print__(text):
        print(text)
        sys.stdout.flush()

    if not os.path.exists(".foiCache"):
        f = open(".foiCache", "a")
        f.close()
