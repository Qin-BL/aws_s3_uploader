import os
import math
import copy
import logging
import time
import uuid
import requests
import constants
from modules.auth.auth_manager import AuthService
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender
from modules.utils.result import Result, NewResult
from robox import config
from turingvideo.stats import stats
from utils import box
from robot.cache import INSPECTIONS_CREATE_EXPIRATION, INSPECTIONS_CREATE_CATEGORY, INSPECTIONS_CREATE_CHECK_INTERVAL
from robot.cache_executor import CacheExecutor

logger = logging.getLogger(__name__)


_ROBOT_STATE_KEY_STATUS = 'status'
_ROBOT_STATE_KEY_MODE = 'mode'
_ROBOT_STATE_KEY_POWER_PERCENTAGE = 'power_percentage'

_ROBOT_STATUS_ONLINE = 'online'
_ROBOT_STATUS_OFFLINE = 'robot_offline'

_STATE_UPDATE_INTERVAL = 10 * 60
# avoid updating too fast when failed for the last time
_FAILED_UPDATE_INTERVAL = 20


class FileStreamUpload(object):
    def __init__(self, f, filesize, point, chunksize):
        self.f = f
        self.point = point
        self.filesize = filesize
        self.len = min(chunksize, self.filesize-point)
        self.f.seek(point)

    def read(self, size=-1):
        left = self.point + self.len - self.f.tell()
        if size < 0 or size > left:
            size = left
        return self.f.read(size)


class RoboxHTTPHandler(ProcessMessageReceiver, ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX_HTTP

    @classmethod
    def initialize(cls, cfg):
        cls._cfg = cfg
        cls._stats = stats.get_client(__name__)
        cls.register_handlers()
        cls.start_msg_loop()
        cls.inspection_executor = CacheExecutor(INSPECTIONS_CREATE_CATEGORY, INSPECTIONS_CREATE_EXPIRATION,
                                                INSPECTIONS_CREATE_CHECK_INTERVAL, cls.handle_create_inspection)
        cls.inspection_executor.start()

        # cls.SAVING_VIDEO = config.WEB_URL + '/api/v1/camera/cameras/saving_video'
        # cls.AUTO_UPDATE = config.WEB_URL + '/api/v1/camera/cameras/auto_update'
        cls.ROBOT_META_TYPE = 'Bot_Nimbo'
        cls.CREATE_VIDEO_SEGMENT = (
            config.WEB_URL + '/api/v1/robot_video/robot_videos'
        )
        cls.CREATE_SNAPSHOT = config.WEB_URL + '/api/v1/robot_snap/robot_snaps'
        cls.CREATE_EVENT = config.WEB_URL + '/api/v1/event/events/robot'
        cls.CREATE_INSPECTION = config.WEB_URL + '/api/v1/inspection/inspections'
        cls.STATE_UPDATE = config.WEB_URL + '/api/v1/robot/robots/update_state'
        cls.ROBOT_META_UPDATE = config.WEB_URL + '/api/v1/robot/robots/update_robot_meta'
        cls.GET_INSPECTION_TARGETS = config.WEB_URL + '/api/v1/target/targets/all_targets'

        cls.INCIDENT_REPORT = config.WEB_URL + '/api/v1/case/cases/incident'
        cls.LIFT_COMMAND = config.GW_URL + '/s/lift/command'

        cls.SENSOR_REPORT = config.GW_URL + '/s/config/sensor/point'
        cls.CONFIG_ROBOT = config.GW_URL + '/s/config/robot/'
        cls.UPLOAD_ROUTINE_HISTORY = config.GW_URL + '/s/config/history/routine_history'

        # mutipart upload
        cls.MULTIPART_UPLOAD_CREATE = config.WEB_URL + '/api/v1/upload/multipart/initiate'
        cls.MULTIPART_UPLOAD_SIGN_URL = config.WEB_URL + '/api/v1/upload/multipart/sign_url'
        cls.MULTIPART_UPLOAD_COMPLETE = config.WEB_URL + '/api/v1/upload/multipart/complete'
        cls.CREATE_SITE_FLOOR = config.WEB_URL + '/api/v1/patrol/site_floor/videos/upload'

        # get jwt token
        cls.OBTAIN_JWT_TOKEN_URL = config.WEB_URL + '/api/v1/robot/robots/obtain_jwt_token'

        cls._last_update_snapshot = 0
        cls._last_update_success = False

        cls._last_state_data = {}
        cls._next_state_update_timestamp = 0
        cls._updated_scene_number = -2
        cls._last_failed_timestamp = None

    @classmethod
    def register_handlers(cls):
        cls.register(constants.EVENT.CLOUD_CREATE, cls.handle_create_event)
        cls.register(
            constants.SNAPSHOT.CLOUD_CREATE, cls.handle_create_snapshot
        )
        cls.register(
            constants.VIDEO_SEGMENT.CLOUD_CREATE,
            cls.handle_create_video_segment,
        )
        cls.register(
            constants.INSPECTION.CLOUD_CREATE,
            cls.handle_create_inspection
        )
        cls.register(
            constants.STATUS.ROBOT_WS, cls.handle_robot_ws_state_update
        )
        cls.register(constants.STATUS.ROBOT_META, cls.handle_update_robot_meta)

        cls.register(constants.STATUS.INCIDENT, cls.handle_incident_report)
        cls.register(constants.STATUS.SENSOR, cls.handle_sensor_report)
        cls.register(constants.STATUS.ROUTINE_HISTORY, cls.handle_upload_routine_history)
        cls.register(constants.ACTION.LIFT_COMMAND, cls.handle_lift_command)
        cls.register(constants.MAPPING.MAP_LOCAL_DELETE, cls.handle_map_local_delete)
        cls.register(constants.MAPPING.MAPS_LOCAL_DELETE, cls.handle_maps_local_delete)

    @classmethod
    def obtain_jwt_token(cls):
        result = AuthService.rest_get(cls.OBTAIN_JWT_TOKEN_URL)
        logger.info("http.obtain_jwt_token", extra={
            "result": result
        })
        return result

    @classmethod
    def multipart_upload_file(cls, signed_url, upload_file, file_size, part_offset, part_chunksize, timeout=None):
        result = NewResult()
        with open(upload_file, "rb") as f:
            # if ConnectionError, retry
            try:
                fsu = FileStreamUpload(f, file_size, part_offset, part_chunksize)
                if timeout is None:
                    timeout = math.ceil(fsu.len / config.DEFAULT_NETWORK_SPEED)
                response = requests.put(signed_url, data=fsu, stream=True, timeout=timeout)
                result.status_code = response.status_code
                result.headers = response.headers
                result.ret = response.content
            except ConnectionError as e:
                result.dm = 'connection_error'
                result.status_code = config.RETRY_CODE
                result.em = str(e)
                logger.error('multipart_upload_file.connection_error', extra={
                    "signed_url": signed_url,
                    "upload_file": upload_file,
                    "file_size": file_size,
                    "part_offset": part_offset,
                    "part_chunksize": part_chunksize,
                    "timeout": timeout,
                    "result": result
                }, stack_info=True, exc_info=True)
            except Exception as e:
                result.dm = 'logic_error'
                result.status_code = 500
                result.em = str(e)
                logger.error('multipart_upload_file.logic_error', extra={
                    "signed_url": signed_url,
                    "upload_file": upload_file,
                    "file_size": file_size,
                    "part_offset": part_offset,
                    "part_chunksize": part_chunksize,
                    "timeout": timeout,
                    "result": result
                }, stack_info=True, exc_info=True)
        return result

    @classmethod
    def multipart_upload_create(cls, upload_filename, file_type):
        data = {
            "filename": upload_filename,
            "type": file_type
        }
        result = AuthService.rest_post(cls.MULTIPART_UPLOAD_CREATE, data=data)
        logger.info(
            "http.multipart.upload.create",
            extra={
                "upload_data": data,
                "response": result
            }
        )
        return result

    @classmethod
    def multipart_upload_sign_url(cls, key, upload_id, part_no):
        data = {
            "Key": key,
            "UploadId": upload_id,
            "PartNumber": part_no
        }
        result = AuthService.rest_post(cls.MULTIPART_UPLOAD_SIGN_URL, data=data)
        logger.info(
            "http.multipart.upload.sign_url",
            extra={
                "request-data": data,
                "response": result
            }
        )
        return result

    @classmethod
    def multipart_upload_complete(cls, key, upload_id, parts):
        data = {
            "Key": key,
            "UploadId": upload_id,
            "Parts": parts,
            "ignoreNotExist": config.S3_IGNORE_NOT_EXIST
        }
        result = AuthService.rest_post(cls.MULTIPART_UPLOAD_COMPLETE, data=data)
        logger.info(
            "http.multipart.upload.complete",
            extra={
                "request-data": data,
                "response": result
            }
        )
        return result

    @classmethod
    def create_site_floor(cls, routine_execution_id, pc_file_data, mobile_file_data, floor_id, taken_at):
        data = {
            "routine_execution_id": routine_execution_id,
            "floor_id": floor_id,
            "taken_at": taken_at,
            "pc_file": pc_file_data,
            "mobile_file": mobile_file_data
        }
        result = AuthService.rest_post(cls.CREATE_SITE_FLOOR, data=data)
        logger.info(
            "http.create_site_floor",
            extra={
                "url": cls.CREATE_SITE_FLOOR,
                "data": data,
                "result": result
            }
        )
        return result

    @classmethod
    def handle_robot_ws_state_update(cls, msg, **kwargs):
        '''
        msg: {
          connected: <bool>
          state: {
            power_percentage: <float>,  # [0, 100]
            mode: <string>
          }
        }
        '''
        state_raw = msg.get('robot', {})
        state_data = {
            _ROBOT_STATE_KEY_STATUS: _ROBOT_STATUS_ONLINE if msg['connected'] else _ROBOT_STATUS_OFFLINE,
            _ROBOT_STATE_KEY_MODE: '',
            _ROBOT_STATE_KEY_POWER_PERCENTAGE: None,
        }
        if state_data[_ROBOT_STATE_KEY_STATUS] == _ROBOT_STATUS_ONLINE:
            state_data[_ROBOT_STATE_KEY_MODE] = state_raw.get('mode', '')
            if 'power_percentage' in state_raw:
                state_data[_ROBOT_STATE_KEY_POWER_PERCENTAGE] = state_raw['power_percentage']
                cls._stats.gauge('robot.state.power_percentage', state_data[_ROBOT_STATE_KEY_POWER_PERCENTAGE])
            else:
                logger.warning('robot.state.update.null.power_percentage', extra=state_raw)

        prev_mode = cls._last_state_data.get(_ROBOT_STATE_KEY_MODE, '')
        prev_status = cls._last_state_data.get(_ROBOT_STATE_KEY_STATUS, '')
        prev_power = (_ROBOT_STATE_KEY_POWER_PERCENTAGE in cls._last_state_data and
            cls._last_state_data[_ROBOT_STATE_KEY_POWER_PERCENTAGE]) or 0
        power_diff = abs(state_raw.get('power_percentage', 0) - prev_power)
        # TODO(gh): this is a stupid if, not scalable
        updated = True
        if prev_status == state_data[_ROBOT_STATE_KEY_STATUS]:
            if prev_status == _ROBOT_STATUS_OFFLINE:
                updated = False
            elif prev_mode == state_data.get(
                'mode', ''
            ) and power_diff < 1:
                updated = False

        logger.debug(
            'robot.state.update.check',
            extra={
                'check-updated': updated,
                'update-msg': msg,
                'state-data': state_data,
                'last-update-state': cls._last_state_data
            }
        )

        now = time.time()
        if (not updated and now < cls._next_state_update_timestamp) \
                or (cls._last_failed_timestamp is not None
                    and now - cls._last_failed_timestamp < _FAILED_UPDATE_INTERVAL):
            return None
        # avoid repeatedly increasing
        if cls._updated_scene_number < AuthService.scene_number:
            with AuthService.scene_lock:
                AuthService.scene_number += 1
                cls._updated_scene_number = AuthService.scene_number

        data = {
            'scene': AuthService.scene_number,
            'state': {
                'robot': {
                    'state': state_data
                }
            },
            'partial': True,
        }
        result = AuthService.rest_post(cls.STATE_UPDATE, data)
        logger.info(
            'http.robox.state.update',
            extra={
                'request-data': data,
                'request-kwargs': kwargs,
                'response-raw': result,
            }
        )
        if result.dm == 'ok':
            cls._last_state_data = state_data
            cls._next_state_update_timestamp = now + _STATE_UPDATE_INTERVAL
            cls._updated_scene_number -= 1
            cls._last_failed_timestamp = None
        else:
            cls._last_failed_timestamp = now

        return result

    @classmethod
    def handle_create_event(cls, msg, **kwargs):
        result = AuthService.rest_post(cls.CREATE_EVENT, msg)
        logger.debug(
            'http.event.create',
            extra={
                'request-data': msg,
                'request-kwargs': kwargs,
                'response-raw': result,
            }
        )

        # TODO(gh): use decorator
        # if result.dm != 'ok':
        #     if 'mretry' not in kwargs:
        #         kwargs['mretry'] = 3
        #     cls.retry_msg(5, msg, **kwargs)

        return result

    @classmethod
    def handle_create_snapshot(cls, msg, **kwargs):
        now = time.time()
        should_update = False
        if not cls._last_update_success or (
            now - cls._last_update_snapshot > config.SNAPSHOT_UPDATE_INTERVAL
        ):
            should_update = True
            cls._last_update_snapshot = now
            cls._last_update_success = False
        msg['should_update_snapshot'] = should_update

        result = AuthService.rest_post(cls.CREATE_SNAPSHOT, msg)
        logger.debug(
            'http.snapshot.create',
            extra={
                'request-data': msg,
                'request-kwargs': kwargs,
                'response-raw': result,
            }
        )

        if should_update and result.raw is not None:
            cls._last_update_success = True

        # TODO(gh): use decorator
        # if result.dm != 'ok':
        #     if 'mretry' not in kwargs:
        #         kwargs['mretry'] = 3
        #     cls.retry_msg(5, msg, **kwargs)
        return result

    @classmethod
    def handle_create_video_segment(cls, msg, **kwargs):
        result = AuthService.rest_post(cls.CREATE_VIDEO_SEGMENT, msg)
        logger.debug(
            'http.segment.create',
            extra={
                'request-data': msg,
                'request-kwargs': kwargs,
                'response-raw': result,
            }
        )

        # TODO(gh): use decorator
        if result.dm != 'ok':
            if 'mretry' not in kwargs:
                kwargs['mretry'] = 3
            cls.retry_msg(5, msg, **kwargs)

        return result

    @classmethod
    def handle_create_inspection(cls, msg, **kwargs):
        result = AuthService.rest_post(cls.CREATE_INSPECTION, msg)
        logger.info(
            'http.inspection.video.create',
            extra={
                'request-data': msg,
                'request-kwargs': kwargs,
                'response-raw': result,
                'response-status_code': result.status_code
            }
        )
        return result

    @classmethod
    def handle_update_robot_meta(cls, msg, **kwargs):
        level = msg.get('level', None)
        version = msg.get('version', None)
        if not level or not version:
            return

        data = {
            'app_agent': {
                'name': cls.ROBOT_META_TYPE,
                'level': int(level),
                'version': version
            }
        }

        result = AuthService.rest_put(cls.ROBOT_META_UPDATE, data)
        logger.debug(
            'http.robot.meta.update',
            extra={
                'request-data': data,
                'request-kwargs': kwargs,
                'response-raw': result,
            }
        )

        return result

    @classmethod
    def handle_incident_report(cls, msg, **kwargs):
        report_msg = copy.copy(msg)
        robox_data = box.load_box_info(cls._cfg)

        report_msg['meta_data'] = {
            'robox': robox_data,
            'robot': msg.get('meta_data'),
        }

        report_msg['system_id'] = robox_data['data']['box-id']
        report_msg['incident_type'] = msg.get('type')
        report_msg['escalation_id'] = str(uuid.uuid4())
        
        logger.info(
            'http.incident.report',
            extra={
                'request-data': report_msg,
            }
        )

        result = AuthService.rest_post(cls.INCIDENT_REPORT, report_msg)
        logger.debug(
            'http.incident.report',
            extra={
                'request-data': report_msg,
                'response-raw': result,
            }
        )

        return result

    @classmethod
    def handle_map_local_delete(cls, msg, **kwargs):
        robot_id = AuthService.get_box_name()
        map_id = msg.get('map_id', None)
        # Call Config Server to delete aws object.
        url = cls.CONFIG_ROBOT + robot_id + "/map/" + map_id
        result = AuthService.rest_delete(url, None)
        logger.debug(
            'http.map.delete',
            extra={
                'request-data': msg,
                'response-raw': result,
            }
        )

        return result

    @classmethod
    def handle_maps_local_delete(cls, msg, **kwargs):
        robot_id = AuthService.get_box_name()
        map_ids = msg.get('map_ids', None)
        # Call Config Server to delete aws object.
        url = cls.CONFIG_ROBOT + robot_id + "/maps"
        body = {
            'map_ids': map_ids
        }
        result = AuthService.rest_delete(url, body)
        logger.info(
            'http.maps.delete',
            extra={
                'request-data': msg,
                'response-raw': result,
            }
        )

        return result

    @classmethod
    def handle_lift_command(cls, msg, **kwargs):
        command = copy.copy(msg)
        command['robot_id'] = AuthService.get_box_name()

        logger.info(
            'http.lift_command.request',
            extra={
                'request-data': command,
            }
        )

        result = AuthService.rest_post(cls.LIFT_COMMAND, command)
        logger.info(
            'http.lift_command.result',
            extra={
                'request-data': command,
                'response-raw': result,
            }
        )
        return_msg = {
            'act': constants.ACTION.LIFT_COMMAND + '!',
            'ret': result.ret
        }
        if result.dm == 'ok':
            return_msg['err'] = {'dm': 'ok', 'ec': 0, 'em': 'ok'}
        # else:
        #     #TODO(nick): add error parsing here
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, return_msg, **kwargs)
        return result

    @classmethod
    def handle_sensor_report(cls, msg, **kwargs):
        logger.info(
            'http.sensor.report',
            extra={
                'request-data': msg,
            }
        )

        result = AuthService.rest_post(cls.SENSOR_REPORT, msg)
        logger.debug(
            'http.sensor.report',
            extra={
                'request-data': msg,
                'response-raw': result,
            }
        )

        return result

    @classmethod
    def handle_upload_routine_history(cls, msg, **kwargs):
        message = copy.copy(msg)
        message['robot_id'] = AuthService.get_box_name()
        logger.info(
            'http.routine_history.upload',
            extra={
                'request-data': message,
            }
        )

        result = AuthService.rest_post(cls.UPLOAD_ROUTINE_HISTORY, message)
        logger.debug(
            'http.routine_history.upload',
            extra={
                'request-data': msg,
                'response-raw': result,
            }
        )

        return result

    @classmethod
    def handle_get_inspection_targets(cls, visible):
        params = dict(visible=visible)
        result = AuthService.rest_get(cls.GET_INSPECTION_TARGETS, params)

        if result.raw['err']['ec'] != 0:
            logger.error(
                'http.get.inspection.targets.fail',
                extra={
                    'response-raw': result,
                }
            )
            return None

        targets = result.ret
        logger.debug(
            'http.get.inspection.targets',
            extra={
                'targets': targets,
                'response-raw': result,
            }
        )
        return targets

    @classmethod
    def enqueue_create_inspection(cls, msg):
        return cls.inspection_executor.enqueue(msg)
