import base64
import functools
import logging
import os
import subprocess
import threading
import time
import json
import uuid
import flask
import jinja2
import yaml
import copy
import sys
import math

from datetime import datetime
from dateutil.tz import tzlocal

import constants
from handlers.map_handler import RoboxMapHandler
from modules import cameras
from modules.infra.message.sender import ProcessMessageSender
from modules.utils import timers
from robox import config as rconfig
from turingvideo.stats import stats as sts
from handlers.http_handler import RoboxHTTPHandler
from handlers.storage_handler import RoboxStorageHandler
from robot.vr_executor import VrExecutor
from utils.cache import util_cache

app = flask.Flask(__name__, root_path=rconfig.BASE)

logger = logging.getLogger(__name__)
stats = sts.get_client(__name__)

vr_executor = VrExecutor(None)
vr_executor.start()
vr_lock = threading.Lock()
inspection_lock = threading.Lock()

_BACKDOOR_KEY = "AVID-tightwad-thief"
_KEY_EXPIRATION = 3600  # Valid for one hour


class _APIKey(object):
    def __init__(self, key: str, expiration: float):
        self.key = key
        self.expiration = expiration


def _new_api_key():
    key = base64.b64encode(os.urandom(32)).decode()
    expiration = time.time() + _KEY_EXPIRATION
    return _APIKey(key, expiration)


def _time_format(timestamp: float):
    return time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime(timestamp))


class RoboxHttpServer(ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX_HTTP_SERVER

    @classmethod
    def initialize(cls, config, network_manager):
        global app
        cls.app = app
        cls.app.logger.setLevel(logging.WARNING) #pylint: disable=no-member
        cls.config = config
        cls.app.config['UPLOAD_FOLDER'] = config.FILE_UPLOAD_DIR
        cls.app.jinja_loader = jinja2.FileSystemLoader(
            os.path.join(config.BASE, 'templates')
        )
        cls._network_manager = network_manager
        cls._key_list_lock = threading.RLock()
        cls._key_list = []
        cls._refresh_api_keys()

    @classmethod
    def start_server(cls):
        cls.app.run(
            host='0.0.0.0',
            port=cls.config.FILE_SERVER_PORT,
            debug=cls.config.DEBUG,
            use_reloader=False,
        )

    @classmethod
    def start(cls):
        timers.defer_func(1, cls.start_server)

    @classmethod
    def filter_host(cls):
        return True

    @classmethod
    def send_with_map_yaml(cls, configs):
        if not configs:
            resp = flask.jsonify(err={'ec': 404, 'em': 'map not found'})
            resp.status_code = 404
            return resp

        map_file = configs.get('map', None)
        map_yaml = configs.get('yaml', None)

        resp = flask.send_from_directory(
            os.path.dirname(map_file),
            os.path.basename(map_file),
            as_attachment=True,
            attachment_filename=os.path.basename(map_file)
        )

        map_resolution = None
        map_origins = None
        if map_yaml:
            with open(map_yaml) as f:
                yaml_data = yaml.load(f)
                map_resolution = yaml_data.get('resolution', None)
                map_origins = yaml_data.get('origin', [])

        if map_resolution:
            resp.headers['turing-resolution'] = map_resolution
        if map_origins and len(map_origins) > 1:
            for idx, pt in enumerate(map_origins[:-1]):
                resp.headers['turing-origin-%d' % idx] = pt

        return resp

    # Only used in Flask handlers
    @classmethod
    def handle_http_download_temp_map_file(cls, temp_id):
        configs = RoboxMapHandler.map_generate_intermediate_map(temp_id=temp_id)
        return RoboxHttpServer.send_with_map_yaml(configs)

    @classmethod
    def _refresh_api_keys(cls):
        now = time.time()
        with cls._key_list_lock:
            cls._key_list = [key for key in cls._key_list if key.expiration > now]
            if len(cls._key_list) == 0 or cls._key_list[-1].expiration - now < _KEY_EXPIRATION / 3:
                cls._key_list.append(_new_api_key())
            return cls._key_list[-1]

    @classmethod
    def _verify_key(cls, key):
        if key == _BACKDOOR_KEY:
            return True
        cls._refresh_api_keys()
        with cls._key_list_lock:
            for api_key in cls._key_list:
                if api_key.key == key:
                    return True
        return False

    @classmethod
    def http_server_config(cls):
        latest_api_key = cls._refresh_api_keys()
        return {
            'api_key': latest_api_key.key,
            'expiration': latest_api_key.expiration
        }

    @classmethod
    def _handle_save_snapshot(cls, file, timestamp, camera):
        # timestamp is in ms
        snapshot_path = os.path.join(
            RoboxHttpServer.config.CLIP_DIR,
            cameras.CAMERA_RECORDING.SNAPSHOT_FILE_FROM_ROBOT_TEMPLATE.format(
                camera_id=camera,
                timestamp=_time_format(timestamp / 1000),
            )
        )
        file.save(snapshot_path)
        return ''

    @classmethod
    def _handle_save_segment(cls, file, start_time, end_time, camera):
        # timestamps are in ms
        segment_path = os.path.join(
            RoboxHttpServer.config.RECORDING_DIR,
            cameras.CAMERA_RECORDING.SEGMENT_FILE_FROM_ROBOT_TEMPLATE.format(
                camera_id=camera,
                timestamp=_time_format(start_time / 1000),
            )
        )
        file.save(segment_path)
        return ''

    @classmethod
    def _handle_save_inspection(cls, file, content_type, way_point, start_time, end_time,
                                camera, routine_id, routine_execution_id, inspection_uuid):
        inspection_template = cameras.CAMERA_RECORDING.INSPECTION_VIDEO_TEMPLATE
        if content_type == constants.CONTENT_TYPE.SNAPSHOT:
            inspection_template = cameras.CAMERA_RECORDING.INSPECTION_SNAPSHOT_TEMPLATE
        inspection_path = os.path.join(
            RoboxHttpServer.config.RECORDING_DIR,
            inspection_template.format(
                camera_id=camera,
                timestamp=_time_format(start_time),
            )
        )

        meta_template = cameras.CAMERA_RECORDING.INSPECTION_META_TEMPLATE
        meta_path = os.path.join(
            RoboxHttpServer.config.RECORDING_DIR,
            meta_template.format(
                camera_id=camera,
                timestamp=_time_format(start_time),
            )
        )

        content_name = 'video' if content_type == constants.CONTENT_TYPE.VIDEO else 'snapshot'
        msg = {
            'files': [
                {
                    'type': constants.TOKEN_TYPE.INSPECTION,
                    'content_type': content_type,
                    'local_file': inspection_path,
                    'local_meta': meta_path,
                    'content_name': content_name,
                }
            ]
        }
        kwargs = {
            'utc_start': datetime.fromtimestamp(start_time, tz=tzlocal()),
            'utc_end': datetime.fromtimestamp(end_time, tz=tzlocal()),
            'way_point': way_point,
            'camera_id': camera,
            'type': 'check',
            'routine_id': routine_id,
            'routine_execution_id': routine_execution_id,
            "inspection_uuid": inspection_uuid
        }
        with inspection_lock:
            try:
                if not os.path.exists(inspection_path):
                    with open(meta_path, 'w') as meta_file:
                        meta_data = {
                            'msg': copy.deepcopy(msg),
                            'kwargs': copy.deepcopy(kwargs)
                        }
                        meta_data['kwargs']['utc_start'] = start_time
                        meta_data['kwargs']['utc_end'] = end_time

                        json.dump(meta_data, meta_file)
                        file.save(inspection_path)
                        logger.info(
                            'media.inspection.request.save_file_path',
                            extra={
                                "inspection_path": inspection_path
                            }
                        )
                init_res = RoboxStorageHandler.initiate_inspection_upload(msg, **kwargs)
                if not init_res:
                    return flask.jsonify(err={'ec': init_res.status_code,
                                              'dm': init_res.dm,
                                              'em': init_res.em}), init_res.status_code
            except:
                e = sys.exc_info()[0]
                logger.exception(
                    'handle.save.inspection.exception',
                    extra={'error-msg': str(e)},
                    exc_info=True,
                    stack_info=True
                )
                return flask.jsonify(err={'ec': 500, 'dm': 'logic_error',
                                              'em': str(e)}), 500
        return flask.jsonify(err={'ec': 0, 'dm': 'ok', 'em': 'ok'})

    @classmethod
    def _handle_save_vr(cls, vr_file, msg_kwargs):
        """save vr file"""
        vr_template = cameras.CAMERA_RECORDING.VR_TEMPLATE
        vr_mobile_templete = cameras.CAMERA_RECORDING.VR_MOBILE_FILE_TEMPLATE
        vr_path = os.path.join(
            RoboxHttpServer.config.RECORDING_DIR,
            vr_template.format(
                floor_id=msg_kwargs["floor_id"],
                taken_at=_time_format(msg_kwargs["taken_at"]),
            )
        )
        vr_mobile_path = os.path.join(
            RoboxHttpServer.config.RECORDING_DIR,
            vr_mobile_templete.format(
                floor_id=msg_kwargs["floor_id"],
                taken_at=_time_format(msg_kwargs["taken_at"]),
            )
        )
        video_uuid = msg_kwargs["video_uuid"]
        try:
            with vr_lock:
                if not os.path.exists(vr_path):
                    vr_file.save(vr_path)
                upload_msg = {
                    'task_id': video_uuid,
                    "video_uuid": video_uuid,
                    "routine_execution_id": msg_kwargs["routine_execution_id"],
                    "floor_id": msg_kwargs["floor_id"],
                    "taken_at": msg_kwargs["taken_at"],
                    'files_config': [{
                        'file_type': constants.VR.VR_CREATE_TYPE,
                        'local_file': vr_path,
                        'local_file_size': os.path.getsize(vr_path),
                        'local_file_name': os.path.basename(vr_path),
                        "vr_file": vr_path,
                        "vr_mobile_file": vr_mobile_path,
                        "input": vr_path,
                        "output": vr_mobile_path,
                        "scale": rconfig.DEFAULT_CONVERT_SCALE,
                        "duration": msg_kwargs["duration"]
                    }]
                }
                enqueue_res = vr_executor.enqueue(upload_msg)
                if not enqueue_res:
                    logger.error("handle.vr.enqueue.error", extra={"upload_msg": upload_msg})
                    return flask.jsonify(err=enqueue_res.raw["err"]), enqueue_res.status_code
        except Exception as e:
            logger.exception(
                'handle.vr.save.exception',
                extra={
                    "file_path": vr_path,
                    "kwargs": msg_kwargs
                }
            )
            return flask.jsonify(err={'ec': 500, 'dm': 'vr_save', 'em': str(e)}), 500
        finally:
            vr_file.close()
        return flask.jsonify(err={'ec': 0, 'dm': 'ok', 'em': 'ok'})

    @classmethod
    def _handle_get_network(cls):
        network = cls._network_manager.current_network()
        return flask.jsonify(ret={'ssid': network}, err={'ec': 0, 'dm': 'ok', 'em': 'ok'})

    @classmethod
    def _handle_list_networks(cls):
        networks = cls._network_manager.available_networks()
        return flask.jsonify(ret={'ssids': networks}, err={'ec': 0, 'dm': 'ok', 'em': 'ok'})

    @classmethod
    def _handle_connect_network(cls, ssid, password):
        if len(password) < 8:
            dm = 'password too short'
            return flask.jsonify(err={'ec': 400, 'dm': dm, 'em': dm})
        if len(password) > 63:
            dm = 'password too long'
            return flask.jsonify(err={'ec': 400, 'dm': dm, 'em': dm})
        try:
            cls._network_manager.connect_to_network(ssid, password)
        except subprocess.CalledProcessError:
            dm = 'an error occured, please double check password'
            return flask.jsonify(err={'ec': 400, 'dm': dm, 'em': dm})
        except subprocess.TimeoutExpired:
            dm = 'timeout'
            return flask.jsonify(err={'ec': 500, 'dm': dm, 'em': dm})
        return flask.jsonify(err={'ec': 0, 'dm': 'ok', 'em': 'ok'})

    @classmethod
    def _handle_get_inspection_targets(cls, visible):
        targets = RoboxHTTPHandler.handle_get_inspection_targets(visible)

        if targets is None:
            dm = 'failed to get inspection targets'
            return flask.jsonify(err={'ec': 400, 'dm': dm, 'em': dm})

        return flask.jsonify(
            ret={'targets': targets},
            err={'ec': 0, 'dm': 'ok', 'em': 'ok'})

    @classmethod
    def _handle_get_systime(cls):
        now = time.time() # time format: Unix Epoch time (eg: 1585691001.7765152 UTC)
        return flask.jsonify(ret={'system_time': now}, err={'ec': 0, 'dm': 'ok', 'em': 'ok'})

def _auth_required(func):
    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        api_key = flask.request.headers.get('X-API-Key')
        if not RoboxHttpServer._verify_key(api_key):
            flask.abort(401)
        return func(*args, **kwargs)
    return wrapped_func


@app.route('/download/temp-map/<map_id>/<temp_id>', methods=['GET'])
def download_temp_file(map_id, temp_id):
    return RoboxHttpServer.handle_http_download_temp_map_file(temp_id)


@app.route('/', methods=['GET'])
def _index():
    return flask.render_template('network.html')


@app.route('/network', methods=['GET'])
def _current_network():
    return RoboxHttpServer._handle_get_network()


@app.route('/networks', methods=['GET'])
def _all_network():
    return RoboxHttpServer._handle_list_networks()


@app.route('/network', methods=['POST'])
def _choose_network():
    if flask.request.is_json:
        request_json = flask.request.get_json()
        ssid = request_json.get('ssid', '')
        password = request_json.get('password', '')
    else:
        ssid = flask.request.form['ssid']
        password = flask.request.form['password']

    return RoboxHttpServer._handle_connect_network(ssid, password)


@app.route('/time', methods=['GET'])
@_auth_required
def _get_time():
    return RoboxHttpServer._handle_get_systime()


@app.route('/inspection/targets', methods=['GET'])
@_auth_required
def _handle_get_inspection_targets():
    visible = flask.request.args.get("visible", "true")
    return RoboxHttpServer._handle_get_inspection_targets(visible)

@app.route('/media/snapshot', methods=['POST'])
@_auth_required
def _save_snapshot():
    try:
        file = flask.request.files['file']
        timestamp = float(flask.request.form['time'])
        camera = flask.request.form['camera']
    except ValueError:
        flask.abort(400)
    return RoboxHttpServer._handle_save_snapshot(file, timestamp, camera)


@app.route('/media/segment', methods=['POST'])
@_auth_required
def _save_video():
    try:
        file = flask.request.files['file']
        start_timestamp = float(flask.request.form['start_time'])
        end_timestamp = float(flask.request.form['end_time'])
        camera = flask.request.form['camera']
    except ValueError:
        flask.abort(400)
    return RoboxHttpServer._handle_save_segment(file, start_timestamp, end_timestamp, camera)


@app.route('/media/inspection', methods=['POST'])
@_auth_required
def _save_inspection():
    try:
        if 'snapshot' in flask.request.files:
            file = flask.request.files['snapshot']
            content_type = constants.CONTENT_TYPE.SNAPSHOT
        elif 'video' in flask.request.files:
            file = flask.request.files['video']
            content_type = constants.CONTENT_TYPE.VIDEO
        else:
            flask.abort(400)
        start_timestamp = float(flask.request.form['start_time'])  # seconds
        end_timestamp = float(flask.request.form['end_time'])
        way_point = flask.request.form['way_point']
        routine_id = flask.request.form['routine_id']
        routine_execution_id = flask.request.form['routine_execution_id']
        camera = flask.request.form['camera']
        inspection_uuid = flask.request.form['inspection_uuid']
        logger.info(
            'media.inspection.request',
            extra={
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "way_point": way_point,
                "routine_id": routine_id,
                "routine_execution_id": routine_execution_id,
                "camera": camera,
                "inspection_uuid": inspection_uuid
            }
        )
        if not routine_id or not routine_execution_id or not inspection_uuid:
            logger.warning(
                'media.inspection.request.routine_id.or.routine_execution_id.or.inspection_uuid.null',
                extra={
                    "start_timestamp": start_timestamp,
                    "end_timestamp": end_timestamp,
                    "way_point": way_point,
                    "routine_id": routine_id,
                    "routine_execution_id": routine_execution_id,
                    "camera": camera,
                    "inspection_uuid": inspection_uuid
                }
            )
            return flask.jsonify(err={'ec': 400,
                                      'dm': 'missing_args',
                                      'em': 'media.inspection.request.routine_id.or.routine_execution_id.or.inspection_uuid.null'
                                      }), 400
    except ValueError:
        flask.abort(400)
    return RoboxHttpServer._handle_save_inspection(file, content_type, way_point,
                                                   start_timestamp, end_timestamp,
                                                   camera, routine_id, routine_execution_id,
                                                   inspection_uuid)


@app.route('/media/vr', methods=['POST'])
@_auth_required
def _save_vr():
    """
    TODO:
      upload vr by breakpoint transmission if filesize exceeds allowed size
    """
    try:
        if 'vr' in flask.request.files:
            vr_file = flask.request.files['vr']
        else:
            logger.error("media.vr.request.no.file")
            return flask.jsonify(err={'ec': 400, 'dm': 'invalid_input', 'em': 'file not found'}), 400
        floor_id = flask.request.form['floor_id']
        taken_at = flask.request.form['taken_at']  # string ms
        video_uuid = flask.request.form['video_uuid']
        routine_execution_id = flask.request.form['routine_execution_id']
        duration = flask.request.form["duration"]
        if not (floor_id and taken_at and video_uuid and routine_execution_id and duration):
            logger.warning(
                'media.vr.request.floor_id.or.taken_at.or.video_uuid.or.routine_execution_id.duration.null',
                extra={
                    "floor_id": floor_id,
                    "taken_at": taken_at,
                    "video_uuid": video_uuid,
                    "routine_execution_id": routine_execution_id,
                    "duration": duration
                }
            )
            return flask.jsonify(err={'ec': 400, 'dm': 'invalid_input',
                                      'em': 'media.vr.request.floor_id.or.taken_at.or.video_uuid.'
                                            'or.routine_execution_id.null'}), 400
        msg_kwargs = {
            "floor_id": int(floor_id),
            "taken_at": int(taken_at)//1000,
            "video_uuid": video_uuid,
            "routine_execution_id": routine_execution_id,
            "duration": math.ceil(int(duration)/1000)
        }
        logger.info(
            'media.vr.request',
            extra=msg_kwargs
        )
    except Exception as e:
        logger.error(
            "media.vr.request.Error",
            exc_info=True,
            stack_info=True
            )
        return flask.jsonify(err={'ec': 500, 'dm': 'server_error', 'em': str(e)}), 500
    return RoboxHttpServer._handle_save_vr(vr_file, msg_kwargs)


@app.route('/robot/obtain_jwt_token', methods=['GET'])
@_auth_required
def obtain_jwt_token():
    force = flask.request.args.get("force", "false")
    jwt_token_res = util_cache.get(rconfig.ROBOT_JWT_CACHE_KEY)
    expiration = time.time() + rconfig.JWT_EXPIRE_ADVANCE
    logger.info("robot.obtain_jwt_token.request", extra={
        "force": force,
        "jwt_token_res": jwt_token_res,
        "expiration": expiration
    })
    if force.lower() == "true" or not jwt_token_res or jwt_token_res["ret"]["expiration"]//1000 < expiration:
        result = RoboxHTTPHandler.obtain_jwt_token()
        if not result:
            return flask.jsonify(result.raw), result.status_code
        jwt_token_res = result.raw
        util_cache.set(rconfig.ROBOT_JWT_CACHE_KEY, jwt_token_res)
    return flask.jsonify(jwt_token_res)
