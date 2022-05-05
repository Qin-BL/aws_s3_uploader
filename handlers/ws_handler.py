import uuid
import copy
import logging
import os
import time
import constants
import wget
import json
import hashlib

from configs import manager
from handlers import http_server
from modules.auth.auth_manager import AuthService
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender
from modules.storage.s3 import S3Storage
from modules.utils.files import get_file_path
from robot.tools import RobotAndroidController
from robox import config
from stats.control_event_log_helper import log_robox_process_from_kwargs
from utils import maps
from modules.utils import datetimes
from modules.utils.network import NetworkWatcher
from modules.robot_controller.device_controller import handle_device_set_power

logger = logging.getLogger(__name__)


class RoboxHandler(ProcessMessageReceiver, ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX
    NAV_STATE_INTERVAL = 5


    @classmethod
    def initialize(cls, cfg, detector_message_id):
        cls._cfg = cfg
        cls.last_snapshot_time = 0
        cls.last_nav_state_update_time = 0
        cls.robot_ws_url = None
        cls.last_robot_ws_connected = None
        cls.last_robot_status = {}

        cls.last_robot_version = None
        cls.last_robot_level = None

        cls._detector_message_id = detector_message_id

        cls.register_handlers()
        cls.start_msg_loop(timeout=15, callback=cls.scheduler)

    @classmethod
    def register_handlers(cls):
        cls.register_proxy(constants.STREAM.START, constants.MESSAGE_ID.ROBOX_CAMERA)
        cls.register_proxy(constants.STREAM.STOP, constants.MESSAGE_ID.ROBOX_CAMERA)

        cls.register(constants.CONFIG.LOAD, cls.handle_load_config)
        cls.register(constants.CONFIG.UPDATE, cls.handle_update_config)
        cls.register(
            constants.CONFIG.CLOUD_UPDATE, cls.handle_cloud_update_config
        )

        cls.register_proxy(constants.ACTION.QUEUE, constants.MESSAGE_ID.ROBOX_ACTION)
        cls.register_proxy(constants.ACTION.MSG_ACTION + '!', constants.MESSAGE_ID.ROBOX_ACTION)
        cls.register(constants.ACTION_TYPE.HEAD_PTZ, cls.handle_head_ptz)
        cls.register('robot.ws', cls.handle_robot_ws_status)
        cls.register(constants.STATUS.ROBOT_WS, cls.handle_robot_state)
        cls.register(constants.ACTION.LIFT_COMMAND, cls.handle_lift_command_request)

        cls.register('robot.ws.open', cls.handle_robot_ws_open)

        cls.register(constants.EVENT.PUSH, cls.handle_event_push)
        cls.register(constants.STATUS.FORCE_UPDATE, cls.force_update_state)

        cls.register(constants.ROBOT_APP.UPDATE, cls.handle_update_robot)
        cls.register(constants.ROBOT_APP.OPEN, cls.handle_open_robot_app)

        cls.register_proxy(constants.MAPPING.MAP_LOCAL_DELETE, constants.MESSAGE_ID.ROBOX_MAP)
        cls.register_proxy(constants.MAPPING.MAPS_LOCAL_DELETE, constants.MESSAGE_ID.ROBOX_MAP)

        cls.register(constants.MODE._INTENSE, cls.handle_intense_mode)

        cls.register_proxy(constants.INCIDENT.ROBOT_REPORT, constants.MESSAGE_ID.ROBOX_INCIDENT)
        cls.register_proxy(constants.SENSOR.ROBOT_REPORT, constants.MESSAGE_ID.ROBOX_SENSOR)

        cls.register(constants.ACTION.QUEUE + '!', cls.handle_action_response)
        cls.register(constants.ROBOT_APP.GO_HOME, cls.handle_go_home)
        cls.register(constants.ROBOT_APP.LEAVE_HOME, cls.handle_leave_home)
        cls.register(constants.ROBOT_APP.USE_MAP_AND_ROUTE, cls.handle_use_map_and_route_request)
        cls.register(constants.MAPPING.USE_MAP_AND_ROUTE+'!', cls.handle_use_map_and_route_response)
        cls.register_proxy(constants.ROBOT_APP.MAP_CLOUD_DELETE, constants.MESSAGE_ID.ROBOX_MAP)
        cls.register_proxy(constants.ROBOT_APP.MAPS_CLOUD_DELETE, constants.MESSAGE_ID.ROBOX_MAP)
        cls.register_proxy(constants.DOWNLOAD_MAP, constants.MESSAGE_ID.ROBOX_MAP)

        cls.register(
            constants.ROBOT_APP.GET_STATES + '!', cls.handle_get_states_data
        )

        cls.register(
            constants.ROBOT_APP.GOTO_MAP, cls.handle_map_goto
        )
        cls.register(
            constants.ROBOT_APP.NAV_UPDATE, cls.handle_nav_update
        )
        cls.register(constants.ROBOT_APP.UPLOAD_ROUTINE_HISTORY, cls.handle_upload_routine_history)

        cls.register(constants.SYSTEMCTL.RESTART, cls.handle_systemctl_restart)
        cls.register(constants.SYSTEMCTL.START, cls.handle_systemctl_start)
        cls.register(constants.SYSTEMCTL.STOP, cls.handle_systemctl_stop)
        cls.register(constants.SYSTEMCTL.ACT, cls.handle_systemctl_operation)

        cls.register(constants.ROBOT_APP.RESET_SLAM, cls.handle_reset_slam)
        cls.register(constants.ROBOT_APP.MICRO_MOVE, cls.handle_micro_move)
        cls.register(constants.ROBOT_APP.CONTINUOUS_MOVE, cls.handle_continuous_move)
        cls.register(constants.ACTION_TYPE.CONTINUOUS_MOVE + '!', cls.handle_continuous_move)
        cls.register(constants.ROBOT_APP.FIXED_ROTATE, cls.handle_fixed_rotate)
        cls.register(constants.ACTION_TYPE.FIXED_ROTATE + '!', cls.handle_fixed_rotate)
        cls.register(constants.ROBOT_APP.LIFT_CALLBACK, cls.handle_lift_callback)
        cls.register(constants.ROBOT_APP.RELOCALIZE, cls.handle_relocalize)

        cls.register(constants.ROBOT_APP.REMOVE_ROUTINE_EXECUTION, cls.handle_remove_routine_execution)
        cls.register(constants.ACTION_TYPE.REMOVE_ROUTINE_EXECUTION + '!',
                     cls.handle_remove_routine_execution_response)
        cls.register(constants.ROBOT_APP.GET_ROUTINE_EXECUTION_QUEUE, cls.handle_get_routine_execution_queue)
        cls.register(constants.ACTION_TYPE.GET_ROUTINE_EXECUTION_QUEUE + '!',
                     cls.handle_get_routine_execution_queue)

        cls.register(constants.ACTION_TYPE.LIFT_CALLBACK + '!', cls.handle_lift_callback_response)
        cls.register(constants.ACTION_TYPE.RESET_SLAM + '!', cls.handle_reset_slam_response)
        cls.register(constants.WISP_ROBOT_PRESENT, cls._handle_wisp_robot_present)
        cls.register(constants.WISP_ROBOT_PRESENT + '!', cls._handle_wisp_robot_present_response)
        cls.register(constants.WISP_ROBOT_VIEW, cls._handle_wisp_robot_view)
        cls.register(constants.WISP_ROBOT_VIEW + '!', cls._handle_wisp_robot_view_response)
        cls.register(constants.HTTP_SERVER_CONFIG, cls._handle_fetch_http_server_config)
        cls.register(constants.UPDATE_DETECTION_CONFIG, cls._handle_update_detection_config)
        cls.register(constants.UPLOAD_MAP, cls._handle_upload_map)
        cls.register(constants.UPLOAD_MAPS, cls._handle_upload_maps)
        cls.register(constants.ROBOT_APP.TAKE_ELEVATOR, cls.handle_take_elevator)
        cls.register(constants.ACTION_TYPE.TAKE_ELEVATOR + '!', cls.handle_take_elevator_response)
        cls.register(constants.ROBOT_APP.START_ROUTINE, cls.handle_start_routine)
        cls.register(constants.ACTION_TYPE.START_ROUTINE + '!', cls.handle_start_routine_response)
        cls.register(constants.ROBOT_APP.RESUME_ROUTINE, cls.handle_resume_routine)
        cls.register(constants.ACTION_TYPE.RESUME_ROUTINE + '!', cls.handle_resume_routine_response)
        cls.register(constants.ROBOT_APP.PAUSE_ROUTINE, cls.handle_pause_routine)
        cls.register(constants.ACTION_TYPE.PAUSE_ROUTINE + '!', cls.handle_pause_routine_response)
        cls.register(constants.ROBOT_APP.COMPLETE_ROUTINE, cls.handle_complete_routine)
        cls.register(constants.ACTION_TYPE.COMPLETE_ROUTINE + '!', cls.handle_complete_routine_response)
        # robot power service
        cls.register(constants.ROBOT_POWER_SERVICE.ACT, cls.handle_powerctl_operation)
        # remote elevator
        cls.register(constants.ROBOT_APP.GOTO_FLOOR_AND_OPEN_DOOR, cls.handle_goto_floor_and_open_door)
        cls.register(constants.ACTION_TYPE.GOTO_FLOOR_AND_OPEN_DOOR + '!', cls.handle_goto_floor_and_open_door)
        # some msg no need to handle
        cls.register(constants.ACTION_TYPE.ROUTINE_EXECUTION_QUEUE_CHANGED, cls.handle_routine_execution_queue_changed)
        # finish s3 task
        cls.register(constants.ROBOT_APP.FINISH_S3_TASK, cls.handle_finish_s3_task)

    @classmethod
    def scheduler(cls):
        now = time.time()
        if now - cls.last_snapshot_time > 15:
            cls.send_msg(constants.MESSAGE_ID.ROBOX_CAMERA, constants.SNAPSHOT.GRAB, {})
            cls.last_snapshot_time = now

    @classmethod
    def terminate(cls):
        cls.stop_msg_loop()

    @classmethod
    def handle_goto_floor_and_open_door(cls, msg, **kwargs):
        logger.info("handle_goto_floor_and_open_door", extra={
            "recv_msg": msg,
            "kwargs": kwargs
        })
        return_msg = copy.deepcopy(msg)
        act = return_msg["act"]
        if act.endswith('!'):
            return_msg["act"] = constants.ROBOT_APP.GOTO_FLOOR_AND_OPEN_DOOR + '!'
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)
        else:
            return_msg["act"] = constants.ACTION_TYPE.GOTO_FLOOR_AND_OPEN_DOOR
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, return_msg, **kwargs)

    @classmethod
    def handle_routine_execution_queue_changed(cls, msg, **kwargs):
        logger.info("handle_routine_execution_queue_changed", extra={
            "recv_msg": msg,
            "kwargs": kwargs
        })
        return_msg = copy.deepcopy(msg)
        return_msg["act"] = constants.ROBOT_APP.ROUTINE_EXECUTION_QUEUE_CHANGED
        robot_id = AuthService.get_box_name()
        if robot_id is None:
            logger.error('handle_routine_execution_queue_changed.invalid_robot_id')
            return
        return_msg["arg"]["robot_id"] = robot_id
        logger.info("handle_routine_execution_queue_changed.return_msg", extra={
            "return_msg": return_msg
        })
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)

    @classmethod
    def handle_finish_s3_task(cls, msg, **kwargs):
        logger.info("handle_finish_s3_task", extra={
            "recv_msg": msg,
            "recv_kwargs": kwargs
        })
        cls.send_msg(constants.MESSAGE_ID.ROBOX_STORAGE, constants.MAPPING.FINISH_S3_TASK, msg, **kwargs)

    @classmethod
    def handle_powerctl_operation(cls, msg, **kwargs):
        logger.info("handle_powerctl_operation", extra={
            "recv_msg": msg,
            "kwargs": kwargs
        })
        act = msg.get("act")
        arg = msg.get("arg", {})
        return_msg = copy.copy(msg)
        return_msg.update(act=act + '!')
        if not arg:
            err = 'no arg'
            return_msg['err'] = {
                'dm': 'invalid_arg',
                'ec': 400,
                'em': err,
            }
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)
            return
        operation = arg.get("operation")
        device = arg.get("device")
        if device in config.NO_RESULT_DEVICES:
            return_msg['err'] = {
                'dm': 'ok',
                'ec': 0,
                'em': 'ok',
            }
            return_msg['ret'] = {}
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg)
            time.sleep(3)
        result = handle_device_set_power(operation, device)
        return_msg['err'] = result.raw['err']
        return_msg['ret'] = result.ret
        logger.info("handle_powerctl_operation", extra={"return_msg": return_msg})
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)

    @classmethod
    def handle_get_states_data(cls, msg, **kwargs):
        logger.debug(
            'map.get.states',
            extra={
                'upload-msg': msg,
            }
        )

    @classmethod
    def handle_nav_update(cls, msg, **kwargs):
        request_id = str(msg.get('id', ''))
        if request_id == '':
            logger.debug('msg=%s does not have id', msg)
            return
        arg = msg.get('arg', None)
        if arg is None:
            logger.debug('msg=%s does not have arg', msg)
            return
        robot_ids = AuthService.get_box_name(),
        if robot_ids is None:
            logger.debug('invalid robot id')
            return
        arg['robot_id'] = robot_ids[0]

        location = arg.get('location', None)
        if location is not None:
            sensor_enabled = arg.get('sensor_enabled', False)
            sensor_types = arg.get('sensor_types', '')
            x = location.get('x', 0)
            y = location.get('y', 0)
            map_id = arg.get('map_id', '')
            route_id = arg.get('route_id', '')
            localization_score = arg.get('localization_score', None)
            if localization_score and 'patrol_performance' in sensor_types:
                location_msg = {
                        'robot_id': robot_ids[0],
                        'sensor_type': 'patrol_performance',
                        'point': {
                            'time': datetimes.get_local_now().isoformat(),
                            'location': {
                                'x': x,
                                'y': y,
                                'map_id': map_id,
                                'route_id': route_id,
                            },
                            'data': {
                                'localization_score': localization_score
                            }
                        },
                    }
                logger.info(
                    'robot.localization_score',
                    extra={'location_msg': location_msg},
                )
                cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.STATUS.SENSOR, location_msg, **kwargs)

            if sensor_enabled:
                noise = arg.get('noise', 0.0)
                sensor_msg = {
                    'robot_id': robot_ids[0],
                    'point': {
                        'sensor_types': sensor_types,
                        'time': datetimes.get_local_now().isoformat(),
                        'location': {
                            'x': x,
                            'y': y,
                            'map_id': map_id,
                            'route_id': route_id,
                        },
                        'data': {
                            'noise': noise
                        }
                    },
                }
                logger.info(
                    'robot.sensor',
                    extra={'sensor_msg': sensor_msg},
                )
                cls.send_msg(constants.MESSAGE_ID.ROBOX_SENSOR, constants.SENSOR.ROBOT_REPORT, sensor_msg, **kwargs)

        now = time.time()
        if now - cls.last_nav_state_update_time < cls.NAV_STATE_INTERVAL:
            return
        return_msg = copy.copy(msg)

        # use local network speed rate
        return_msg.setdefault('arg', dict())['signal_strength'] = NetworkWatcher.get_network_rate()

        logger.info(
            'robot.app.nav.update',
            extra={'return_msg': return_msg},
        )
        return_msg['act'] = 'robot.' + msg['act']
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg)
        cls.last_nav_state_update_time = now

    @classmethod
    def handle_systemctl_restart(cls, msg, **kwargs):
        return cls.handle_systemctl_general(msg, constants.SYSTEMCTL_OPERATIONS.RESTART, **kwargs)

    @classmethod
    def handle_systemctl_start(cls, msg, **kwargs):
        return cls.handle_systemctl_general(msg, constants.SYSTEMCTL_OPERATIONS.START, **kwargs)

    @classmethod
    def handle_systemctl_stop(cls, msg, **kwargs):
        return cls.handle_systemctl_general(msg, constants.SYSTEMCTL_OPERATIONS.STOP, **kwargs)

    @classmethod
    def handle_systemctl_operation(cls, msg, **kwargs):
        logger.info("handle_systemctl_operation", extra={
            "recv_msg": msg,
            "kwargs": kwargs
        })
        act = msg.get("act")
        arg = msg.get("arg", {})
        return_msg = copy.copy(msg)
        return_msg.update(act=act + '!')
        return_msg['msg'] = {}
        if not arg:
            err = 'no arg'
            return_msg['err'] = {
                'dm': 'invalid_arg',
                'ec': 400,
                'em': err,
            }
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)
            return
        operation = arg.get("operation")
        cls.handle_systemctl_general(msg, operation, **kwargs)

    @classmethod
    def handle_systemctl_general(cls, msg, operation, **kwargs):
        logger.info("RoboxHandler.handle_systemctl_general", extra={
            "recv_msg": msg,
            "operation": operation,
            "kwargs": kwargs
        })
        arg = msg.get('arg', {})
        service = arg.get('service', 'zenod')

        if service in config.BOX_SERVICES:
            cls.handle_box_systemctl_general(service, msg, operation, **kwargs)
        elif service in config.APP_SERVICES:
            cls.handle_app_systemctl_general(service, msg, operation, **kwargs)
        else:
            err = 'service ' + service + ' is not recognized!'
            logger.error(err)
            ret_msg = msg
            ret_msg['act'] += '!'
            ret_msg['err'] = {
                'dm': 'invalid_service',
                'ec': 400,
                'em': err,
            }
            ret_msg['ret'] = {}
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg, **kwargs)
            return

    @classmethod
    def handle_box_systemctl_general(cls, service, msg, operation, **kwargs):
        logger.info("RoboxHandler.handle_box_systemctl_general", extra={
            "service": service,
            "recv_msg": msg,
            "operation": operation
        })
        try:
            cmd = 'sudo systemctl %s %s' % (operation, service)
            logger.info("handle_box_systemctl_general", extra={"command": cmd})
            if service == 'robox':
                ret_msg = msg
                ret_msg['act'] += '!'
                ret_msg['err'] = {
                    'dm': 'ok',
                    'ec': 0,
                    'em': 'ok',
                }
                ret_msg['ret'] = {}
                cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)
            result = os.system(cmd)
            logger.warning(
                "RoboxHandler.handle_box_systemctl_general",
                extra={
                    'service': service,
                    "operation": operation,
                    'result': result
                },
            )
            result = result == 0
        except Exception:
            logger.error('%s operation ' % service + operation, exc_info=True)
            result = False

        if result:
            ret_msg = msg
            ret_msg['act'] += '!'
            ret_msg['err'] = {
                'dm': 'ok',
                'ec': 0,
                'em': 'ok',
            }
            ret_msg['ret'] = {}
        else:
            ret_msg = msg
            ret_msg['act'] += '!'
            ret_msg['err'] = {
                'dm': 'cmd_error',
                'ec': 400,
                'em': 'cmd error',
            }
            ret_msg['ret'] = {}
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_app_systemctl_general(cls, service, msg, operation, **kwargs):
        logger.info("RoboxHandler.handle_app_systemctl_general", extra={
            "service": service,
            "recv_msg": msg,
            "operation": operation
        })
        service_config = config.APP_SERVICES.get(service, None)
        service_ip = service_config["ip"]
        app = service_config['app']
        main_activity = service_config['main_activity']
        adb_name = service_ip + ':5555'

        cmd_connect = 'adb connect ' + adb_name
        cmd_disconnect = 'adb disconnect ' + adb_name
        cmd_start = (
            'adb -s ' + adb_name + ' shell am start'
            ' -n ' + main_activity +
            ' -a android.intent.action.MAIN'
            ' -c android.intent.category.LAUNCHER'
        )
        cmd_stop = 'adb -s ' + adb_name + ' shell am force-stop ' + app

        cmds = []
        cmds.append(cmd_connect)

        if operation == constants.SYSTEMCTL_OPERATIONS.START:
            cmds.append(cmd_start)
        elif operation == constants.SYSTEMCTL_OPERATIONS.STOP:
            cmds.append(cmd_stop)
        elif operation == constants.SYSTEMCTL_OPERATIONS.RESTART:
            cmds.append(cmd_stop)
            cmds.append(cmd_start)
        else:
            err = '%s operation ' % service + operation + ' is not recognized!'
            logger.error(err)
            ret_msg = msg
            ret_msg['act'] += '!'
            ret_msg['err'] = {
                'dm': "invalid_operation",
                'ec': 400,
                'em': err,
            }
            ret_msg['ret'] = {}
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)
            return

        cmds.append(cmd_disconnect)

        cmd = ' && '.join(cmds)

        try:
            logger.info(cmd)
            result = os.system(cmd)
            logger.warning(
                "RoboxHandler.handle_app_systemctl_general",
                extra={
                    'service': service,
                    "operation": operation,
                    'result': result
                },
            )
            result = result == 0
        except Exception:
            logger.error('%s operation ' % service + operation + ' error!', exc_info=True)
            result = False

        if result:
            ret_msg = msg
            ret_msg['act'] += '!'
            ret_msg['err'] = {
                'dm': 'ok',
                'ec': 0,
                'em': 'ok',
            }
            ret_msg['ret'] = {}
        else:
            ret_msg = msg
            ret_msg['act'] += '!'
            ret_msg['err'] = {
                'dm': 'cmd_error',
                'ec': 400,
                'em': 'cmd error',
            }
            ret_msg['ret'] = {}
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    @log_robox_process_from_kwargs(should_finalize=False)
    def handle_use_map_and_route_request(cls, msg, **kwargs):
        request_id = str(msg.get('id', ''))
        if not cls.last_robot_ws_connected:
            return_msg = msg
            return_msg['act'] += '!'
            return_msg['ret'] = {}
            return_msg['err'] = {
                'dm': 'robot_offline',
                'ec': 400,
                'em': 'robot_offline',
            }
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg)
            return

        return_msg = {
            'id': request_id,
            'act': constants.MAPPING.USE_MAP_AND_ROUTE,
            'arg': msg.get('arg', {}),
            'ebd': {},
            'ext': {},
        }
        if 'ebd' in msg:
            return_msg['ebd'] = msg['ebd']
        logger.info('robox.maproute.request', extra=return_msg)
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, return_msg, **kwargs)

    @classmethod
    def handle_use_map_and_route_response(cls, msg, **kwargs):
        return_msg = msg
        return_msg['act'] = constants.ROBOT_APP.USE_MAP_AND_ROUTE + '!'
        return_msg['ret'] = {}
        logger.info('robox.maproute.response', extra=return_msg)
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)

    @classmethod
    def force_update_state(cls):
        cls.update_state(force=True)

    @classmethod
    def update_state(cls, force=False):
        data = {
            'connected': cls.last_robot_ws_connected,
            'robot': cls.last_robot_status
        }
        robot_version = cls.last_robot_status.get('robot_version', None)
        robot_level = cls.last_robot_status.get('robot_level', None)

        cls._cfg.set_config('robot.version', robot_version)
        cls._cfg.set_config('robot.level', robot_level)

        if robot_version and robot_level:
            if robot_version != cls.last_robot_version or robot_level != cls.last_robot_level:
                cls.last_robot_version = robot_version
                cls.last_robot_level = robot_level
                cls.send_msg(
                    constants.MESSAGE_ID.ROBOX_HTTP, constants.STATUS.ROBOT_META,
                    {'level': robot_level,
                     'version': robot_version}
                )
            if int(robot_level) < int(config.ROBOT_LEVEL):
                cls.send_msg(
                    constants.MESSAGE_ID.ROBOT_WS, constants.GENERAL_MSG.API_MISMATCH, {})

        cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.STATUS.ROBOT_WS, data)

    @classmethod
    def handle_robot_state(cls, msg, **kwargs):
        data = msg.pop('arg', {})
        cls.last_robot_status = data

        ret_data = msg
        ret_data['act'] += '!'
        ret_data['ret'] = {}
        ret_data['err'] = {'dm': 'ok', 'ec': 0, 'em': 'ok'}

        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_data, **kwargs)

        cls.update_state(force=True)

    @classmethod
    def handle_upload_routine_history(cls, msg, **kwargs):
        logger.info(
            'robot.app.upload_routine_history.request',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg.pop('arg', {})

        cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.STATUS.ROUTINE_HISTORY, arg, **kwargs)

    @classmethod
    def handle_lift_command_request(cls, msg, **kwargs):
        logger.info(
            'robot.lift_command.request',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg.pop('arg', {})

        cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.ACTION.LIFT_COMMAND, arg, **kwargs)

    @classmethod
    def handle_lift_command_response(cls, msg, **kwargs):
        logger.info(
            'robot.lift_command.response',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def handle_lift_callback(cls, msg, **kwargs):
        logger.info(
            'robot.lift_callback',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        cmd = {
            'id': msg['id'],
            'arg': msg.get('arg', {}),
            'act': constants.ACTION_TYPE.LIFT_CALLBACK,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot',
                     **kwargs)

    @classmethod
    def handle_lift_callback_response(cls, msg, **kwargs):
        ret_msg = msg
        ret_msg['act'] = constants.ROBOT_APP.LIFT_CALLBACK + '!'
        robot_id = AuthService.get_box_name()
        if robot_id is None:
            logger.error('handle_lift_callback_response.invalid_robot_id')
            return
        ret = ret_msg.get('ret') or dict()
        ret['robot_id'] = robot_id
        ret_msg['ret'] = ret
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg,
                     **kwargs)

    @classmethod
    def handle_start_routine(cls, msg, **kwargs):
        logger.info(
            'robot.start_routine',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg.get('arg', {})
        cmd = {
            'id': msg['id'],
            'arg': arg,
            'act': constants.ACTION_TYPE.START_ROUTINE,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def handle_start_routine_response(cls, msg, **kwargs):
        ret_msg = msg
        ret_msg['act'] = constants.ROBOT_APP.START_ROUTINE + '!'
        robot_id = AuthService.get_box_name()
        if robot_id is None:
            logger.error('handle_start_routine_response.invalid_robot_id')
            return
        ret = ret_msg.get('ret') or dict()
        ret['robot_id'] = robot_id
        ret_msg['ret'] = ret
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_resume_routine(cls, msg, **kwargs):
        logger.info(
            'robot.resume_routine',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg.get('arg', {})
        cmd = {
            'id': msg['id'],
            'arg': arg,
            'act': constants.ACTION_TYPE.RESUME_ROUTINE,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def handle_resume_routine_response(cls, msg, **kwargs):
        ret_msg = msg
        ret_msg['act'] = constants.ROBOT_APP.RESUME_ROUTINE + '!'
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_pause_routine(cls, msg, **kwargs):
        logger.info(
            'robot.pause_routine',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg.get('arg', {})
        cmd = {
            'id': msg['id'],
            'arg': arg,
            'act': constants.ACTION_TYPE.PAUSE_ROUTINE,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def handle_pause_routine_response(cls, msg, **kwargs):
        ret_msg = msg
        ret_msg['act'] = constants.ROBOT_APP.PAUSE_ROUTINE + '!'
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_complete_routine(cls, msg, **kwargs):
        logger.info(
            'robot.complete_routine',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg.get('arg', {})
        cmd = {
            'id': msg['id'],
            'arg': arg,
            'act': constants.ACTION_TYPE.COMPLETE_ROUTINE,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def handle_complete_routine_response(cls, msg, **kwargs):
        ret_msg = msg
        ret_msg['act'] = constants.ROBOT_APP.COMPLETE_ROUTINE + '!'
        robot_id = AuthService.get_box_name()
        if robot_id is None:
            logger.error('handle_complete_routine_response.invalid_robot_id')
            return
        ret = ret_msg.get('ret') or dict()
        ret['robot_id'] = robot_id
        ret_msg['ret'] = ret
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_take_elevator(cls, msg, **kwargs):
        logger.info(
            'robot.take_elevator',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg.get('arg', {})
        lift_from = arg.get('lift_from', None)
        lift_to = arg.get('lift_to', None)
        cmd = {
            'id': 0,
            'arg': {
                'from': lift_from,
                'to': lift_to
            },
            'act': constants.ACTION_TYPE.TAKE_ELEVATOR,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot',
                     **kwargs)

    @classmethod
    def handle_take_elevator_response(cls, msg, **kwargs):
        ret_msg = msg
        ret_msg['act'] = constants.ROBOT_APP.TAKE_ELEVATOR + '!'
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg,
                     **kwargs)

    @classmethod
    def handle_update_robot(cls, msg, **kwargs):
        arg = msg.get('arg', {})
        s3_url = arg.get('url', '')

        ret_data = msg
        ret_data['act'] += '!'
        ret_data['ret'] = {}
        ret_data['err'] = {'dm': 'ok', 'ec': 0, 'em': 'ok'}

        if not s3_url:
            ret_data['err'] = {
                'dm': 'invalid',
                'ec': 400,
                'em': 'invalid',
            }
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_data, **kwargs)
            return

        apk_file = get_file_path(target_dir='/tmp', suffix='.apk')
        try:
            md5_check = arg.get('md5', '')
            wget.download(s3_url, out=apk_file, bar=None)

            if not os.path.exists(apk_file):
                raise ValueError('apk file not downloaded.')

            # TODO(gh): check md5 hash for downloaded file
            robot_ip = cls._cfg.get_config(config.CONFIG_ROBOT_IP_KEY, '')
            if not robot_ip:
                raise ValueError('Robot is not found.')

            controller = RobotAndroidController(
                robot_ip, default_timeout_ms=15 * 1000
            )

            with controller:
                controller.install_apk(apk_file)
                controller.open_app()

        except Exception as e:
            logger.exception(
                'robot.app.update.exception',
                extra={'err-msg': str(e)},
                exc_info=True,
                stack_info=True
            )
            ret_data['err'] = {
                'dm': 'failed',
                'ec': 400,
                'em': str(e),
            }
        finally:
            os.unlink(apk_file)
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_data, **kwargs)

    @classmethod
    def handle_open_robot_app(cls, msg, **kwargs):
        ret_data = msg
        ret_data['act'] += '!'
        ret_data['ret'] = {}
        ret_data['err'] = {'dm': 'ok', 'ec': 0, 'em': 'ok'}

        try:
            robot_ip = cls._cfg.get_config(config.CONFIG_ROBOT_IP_KEY, '')
            if not robot_ip:
                raise ValueError('Robot is not found.')

            controller = RobotAndroidController(
                robot_ip, default_timeout_ms=15 * 1000
            )

            with controller:
                controller.open_app()

        except Exception as e:
            logger.exception(
                'robot.app.open.exception',
                extra={'err-msg': str(e)},
                exc_info=True,
                stack_info=True
            )
            ret_data['err'] = {
                'dm': 'failed',
                'ec': 400,
                'em': str(e),
            }
        finally:
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_data, **kwargs)

    @classmethod
    def handle_robot_ws_status(cls, msg, **kwargs):
        updated = False
        cls.robot_ws_url = msg.get('url', None)
        if cls.last_robot_ws_connected is None or (
            cls.last_robot_ws_connected != bool(msg['connected'])
        ):
            cls.last_robot_ws_connected = bool(msg['connected'])
            updated = True
        cls.update_state(force=True)

    @classmethod
    def build_detector_config(
        cls, camera_config, robot_config, mode=None, skip=None
    ):
        '''
        mode: None, -1, 'on' | 'off'
        '''
        detect_params = robot_config.get('detect_params', {})
        timezone = robot_config.get('timezone')

        if mode is None:
            current_detect_enabled = detect_params.get('detect_enabled', False)
            mode = (
                constants.DETECTION_MODE.ON
                if current_detect_enabled else constants.DETECTION_MODE.OFF
            )

        detector_config = {
            'uri': camera_config.get(manager.CAMERA_CONFIG_KEY_DETECTION_URI, ''),
            'mode': mode
        }

        if skip is not None:
            detector_config['skip'] = skip

        if 'threshold' in detect_params:
            detector_config['threshold'] = detect_params.get('threshold')

        if 'algos' in detect_params:
            detector_config['algos'] = detect_params.get('algos')

        if timezone:
            detector_config['timezone'] = timezone

        return detector_config

    @classmethod
    def update_robot_params(cls, robot_config):
        patrol_params = robot_config.get('patrol_params', {})

        update_msg = {
            'arg': {
                'patrol_params': patrol_params,
            },
            'act': 'update_params',
            'id': 0,
            'ext': {},
            'ebd': {},
        }

        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, update_msg, device='robot')

    @classmethod
    def update_detector_configs(
        cls,
        msg,
        method=constants.CONFIG.UPDATE,
        mode=None,
        skip=None,
        **kwargs
    ):
        camera = manager.RoboxConfigManager.get_camera(camera_id='main')
        robot_config = msg.get('robot', {})

        cls.send_msg(
            'detector.main_camera',
            method,
            cls.build_detector_config(
                camera, robot_config, mode=mode, skip=skip
            ),
            **kwargs,
        )

    @classmethod
    @log_robox_process_from_kwargs(should_finalize=True)
    def handle_load_config(cls, msg, **kwargs):
        '''Called after ConfigManager.on_config_changed'''
        logger.info(
            'config.load', extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        cls.update_detector_configs(
            msg, method=constants.CONFIG.LOAD, **kwargs)
        S3Storage.start()

        camera = manager.RoboxConfigManager.get_camera(camera_id='main')
        cls.send_msg(
            'recorder.cloud.main_camera', constants.RECORDER.BACKGROUND_START,
            camera
        )

    @classmethod
    @log_robox_process_from_kwargs(should_finalize=True)
    def handle_update_config(cls, msg, **kwargs):
        logger.info(
            'config.update', extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        message_body = msg.get('arg', {})
        manager.RoboxConfigManager.on_config_changed(message_body)

    @classmethod
    def handle_cloud_update_config(cls, msg, **kwargs):
        '''Called after ConfigManager.on_config_changed'''
        logger.info(
            'config.cloud.update',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )

        cls.update_detector_configs(msg, **kwargs)

        robot_config = msg.get('robot', {})
        cls.update_robot_params(robot_config)

    @classmethod
    def handle_intense_mode(cls, msg, **kwargs):
        '''
        msg: {
          mode: 'on' | 'off'
        }
        '''
        # Get Mode and negate it
        logger.warning('robox.mode.intense', extra={'payload-msg': msg})
        skip_mode = msg.get('mode', constants.MODE.OFF)

        cls.update_detector_configs(msg, skip=skip_mode, **kwargs)

    @classmethod
    def handle_gps_started(cls, msg, **kwargs):
        msg['act'] += '!'
        msg['ret'] = {}
        msg['err'] = {'dm': 'ok', 'ec': 0, 'em': ''}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def handle_gps_stopped(cls, msg, **kwargs):
        msg['act'] += '!'
        msg['ret'] = {}
        msg['err'] = {'dm': 'ok', 'ec': 0, 'em': ''}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def handle_ranger_started(cls, msg, **kwargs):
        msg['act'] += '!'
        msg['ret'] = {}
        msg['err'] = {'dm': 'ok', 'ec': 0, 'em': ''}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def handle_ranger_stopped(cls, msg, **kwargs):
        msg['act'] += '!'
        msg['ret'] = {}
        msg['err'] = {'dm': 'ok', 'ec': 0, 'em': ''}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def handle_lidar_started(cls, msg, **kwargs):
        msg['act'] += '!'
        msg['ret'] = {}
        msg['err'] = {'dm': 'ok', 'ec': 0, 'em': ''}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def handle_lidar_stopped(cls, msg, **kwargs):
        msg['act'] += '!'
        msg['ret'] = {}
        msg['err'] = {'dm': 'ok', 'ec': 0, 'em': ''}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def handle_head_ptz(cls, msg, **kwargs):
        ptz_action = copy.copy(msg)
        ptz_action['act'] = constants.ACTION.MSG_ACTION
        ptz_action['arg']['type'] = constants.ACTION_TYPE.HEAD_PTZ
        ptz_action['arg']['uuid'] = '1'
        cls.send_msg(
            constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ptz_action, device='robot', **kwargs
        )

    @classmethod
    def handle_robot_ws_open(cls, msg, **kwargs):
        camera_config = manager.RoboxConfigManager.get_camera(camera_id='main')
        robot_config = manager.RoboxConfigManager.get_robot_config()

        cls.handle_robot_ws_status({'connected': True}, **kwargs)

        cls._send_time_sync()

        cls.update_robot_params(robot_config)

    @classmethod
    def _send_time_sync(cls):
        msg = {
            'id': str(uuid.uuid4()),
            'act': 'sync_clock',
            'arg': {
                'time': int(time.time())
            },
            'ext': {},
            'ebd': {},
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg)

    @classmethod
    def handle_event_push(cls, msg, **kwargs):
        if not manager.RoboxConfigManager.get_auto_speak():
            return

        event_source = kwargs.get('event_source', 'main_camera')
        event_start = msg.get('utc_start')

        types = msg.get('types', [])

        if len(types) < 1:
            return

        msg = {
            'id': 15,
            'act': constants.EVENT.PUSH_EVENT,
            'arg': {
                'timestamp': event_start.timestamp(),
                'types': types,
                'camera': event_source,
            },
            'ext': {},
            'ebd': {},
        }

        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, device='robot', **kwargs)

    @classmethod
    def handle_go_home(cls, msg, **kwargs):
        actions = {
            'arg': {
                'orders': [
                    {
                        'name': constants.ACTION_TYPE.GO_HOME,
                    },
                ]
            },
        }
        cls.send_msg(
            constants.MESSAGE_ID.ROBOX_ACTION, constants.ACTION.QUEUE, actions, request=msg, **kwargs
        )

    @classmethod
    def handle_relocalize(cls, msg, **kwargs):
        logger.info(
            'handle_relocalize', extra={
                'msg-msg': msg,
                'msg-kwargs': kwargs,
            }
        )
        msg["act"] = constants.ACTION_TYPE.RELOCALIZE
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, device='robot', **kwargs)

    @classmethod
    def handle_remove_routine_execution(cls, msg, **kwargs):
        logger.info(
            'handle_remove_routine_execution', extra={
                'msg-msg': msg,
                'msg-kwargs': kwargs,
            }
        )
        msg["act"] = constants.ACTION_TYPE.REMOVE_ROUTINE_EXECUTION
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, msg, device='robot', **kwargs)

    @classmethod
    def handle_remove_routine_execution_response(cls, msg, **kwargs):
        logger.info(
            'handle_remove_routine_execution_response', extra={
                'msg-msg': msg,
                'msg-kwargs': kwargs,
            }
        )
        msg['act'] = constants.ROBOT_APP.REMOVE_ROUTINE_EXECUTION + '!'
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, msg)

    @classmethod
    def handle_get_routine_execution_queue(cls, msg, **kwargs):
        logger.info(
            'handle_get_routine_execution_queue', extra={
                'msg-msg': msg,
                'msg-kwargs': kwargs,
            }
        )
        return_msg = copy.deepcopy(msg)
        act = return_msg["act"]
        if act.endswith('!'):
            return_msg["act"] = constants.ROBOT_APP.GET_ROUTINE_EXECUTION_QUEUE + '!'
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)
        else:
            return_msg["act"] = constants.ACTION_TYPE.GET_ROUTINE_EXECUTION_QUEUE
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, return_msg, **kwargs)

    @classmethod
    def handle_leave_home(cls, msg, **kwargs):
        cmd = {
            'id': 0,
            'arg': {},
            'act': constants.ACTION_TYPE.LEAVE_HOME,
            'ext': {},
            'ebd': {},
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

        ret_msg = msg
        ret_msg['act'] += '!'
        ret_msg['err'] = {
            'dm': 'ok',
            'ec': 0,
            'em': 'ok',
        }
        ret_msg['ret'] = {}
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_action_response(cls, msg, **kwargs):
        request = kwargs.get('request', None)
        if request is None:
            return

        ret_msg = request
        ret_msg['act'] += '!'
        ret_msg['err'] = msg['err']
        ret_msg['ret'] = msg['ret']

        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_map_goto(cls, msg, **kwargs):
        args = msg.get('arg', {})

        position_data = {
            "name": "map.position",
            "x": args.get('x', None),
            "y": args.get('y', None),
            "async": True,
            "path_mode": args.get("path_mode", None)
        }
        if 'map_id' in args:
            position_data['map_id'] = args['map_id']

        action_msg = {
            'id': uuid.uuid4(),
            'act': 'robot.directive.orders',
            'arg': {
                'orders': [
                    position_data,
                ]
            },
            'ebd': {},
            'ext': {},
        }
        logger.info("RoboxHandler.handle_map_goto", extra={
            "recv_args": args,
            "position_data": position_data,
            "action_msg": action_msg
        })
        cls.send_msg(
            constants.MESSAGE_ID.ROBOX_ACTION,
            constants.ACTION.QUEUE,
            action_msg,
            request=msg,
            **kwargs
        )

    @classmethod
    def handle_micro_move(cls, msg, **kwargs):
        cmd = {
            'id': 0,
            'arg': msg.get('arg', {}),
            'act': constants.ACTION_TYPE.MICRO_MOVE,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def handle_continuous_move(cls, msg, **kwargs):
        logger.info("handle_continuous_move", extra={
            "recv_msg": msg,
            "kwargs": kwargs
        })
        return_msg = copy.deepcopy(msg)
        act = return_msg["act"]
        if act.endswith('!'):
            return_msg["act"] = constants.ROBOT_APP.CONTINUOUS_MOVE + '!'
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)
        else:
            return_msg["act"] = constants.ACTION_TYPE.CONTINUOUS_MOVE
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, return_msg, device='robot', **kwargs)

    @classmethod
    def handle_fixed_rotate(cls, msg, **kwargs):
        logger.info("handle_fixed_rotate", extra={
            "recv_msg": msg,
            "kwargs": kwargs
        })
        return_msg = copy.deepcopy(msg)
        act = return_msg["act"]
        if act.endswith('!'):
            return_msg["act"] = constants.ROBOT_APP.FIXED_ROTATE + '!'
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg, **kwargs)
        else:
            return_msg["act"] = constants.ACTION_TYPE.FIXED_ROTATE
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, return_msg, device='robot', **kwargs)

    @classmethod
    def handle_reset_slam(cls, msg, **kwargs):
        arg = msg.get('arg', {})
        # Setting them to be zero and will be ignored.
        arg['optimization_interval'] = 0
        arg['trim_number'] = 0

        cmd = {
            'id': 0,
            'arg': arg,
            'act': constants.ACTION_TYPE.RESET_SLAM,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        logger.info(
            'reset.slam', extra={
                'msg-msg': msg,
                'msg-kwargs': kwargs,
                'cmd': cmd,
            }
        )
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def handle_reset_slam_response(cls, msg, **kwargs):
        ret_msg = msg
        ret_msg['act'] = constants.ROBOT_APP.RESET_SLAM + '!'
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    @log_robox_process_from_kwargs(should_finalize=True)
    def _handle_wisp_robot_present(cls, msg, **kwargs):
        return_msg = msg.copy()
        return_msg['act'] += '!'
        return_msg['ret'] = {
            'confirm': False,
        }
        if not cls.last_robot_ws_connected:
            return_msg['err'] = {
                'dm': 'robot_offline',
                'ec': 400,
                'em': 'robot_offline',
            }
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg)
            return

        return_msg['err'] = {
            'dm': 'ok',
            'ec': 0,
            'em': 'ok',
        }
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg)

        arg = msg.get('arg', {})
        arg['wisp_endpoint'] = config.WISP_ENDPOINT
        cmd = {
            'id': msg.get('id', 0),
            'act': constants.WISP_ROBOT_PRESENT,
            'arg': arg,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def _handle_wisp_robot_present_response(cls, msg, **kwargs):
        ret_msg = {
            'id': msg.get('id', 0),
            'act': constants.WISP_ROBOT_PRESENT + '!',
            'ret': {
                'confirm': True,
            },
            'err': msg.get('err', {}),
            'ext': {},
            'ebd': msg.get('ebd', {})
        }
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    @log_robox_process_from_kwargs(should_finalize=True)
    def _handle_wisp_robot_view(cls, msg, **kwargs):
        return_msg = msg.copy()
        return_msg['act'] += '!'
        return_msg['ret'] = {
            'confirm': False,
        }
        if not cls.last_robot_ws_connected:
            return_msg['err'] = {
                'dm': 'robot_offline',
                'ec': 400,
                'em': 'robot_offline',
            }
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg)
            return

        return_msg['err'] = {
            'dm': 'ok',
            'ec': 0,
            'em': 'ok',
        }
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, return_msg)

        arg = msg.get('arg', {})
        arg['wisp_endpoint'] = config.WISP_ENDPOINT
        cmd = {
            'id': msg.get('id', 0),
            'act': constants.WISP_ROBOT_VIEW,
            'arg': arg,
            'ext': {},
            'ebd': msg.get('ebd', {}),
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, cmd, device='robot', **kwargs)

    @classmethod
    def _handle_wisp_robot_view_response(cls, msg, **kwargs):
        ret_msg = {
            'id': msg.get('id', 0),
            'act': constants.WISP_ROBOT_VIEW + '!',
            'ret': {
                'confirm': True,
            },
            'err': msg.get('err', {}),
            'ext': {},
            'ebd': msg.get('ebd', {})
        }
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def _handle_fetch_http_server_config(cls, msg, **kwargs):
        config = http_server.RoboxHttpServer.http_server_config()
        ret_msg = {
            'id': msg.get('id', 0),
            'act': msg['act'] + '!',
            'ret': config,
            'err': {
                'dm': 'ok',
                'ec': 0,
                'em': 'ok',
            },
            'ext': {},
            'ebd': msg.get('ebd', {})
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg, **kwargs)

    @classmethod
    def _handle_update_detection_config(cls, msg, **kwargs):
        cls.send_msg(cls._detector_message_id, constants.DETECTOR_CONFIG_UPDATE, msg)

    @classmethod
    def get_map_file_name(cls, path):
        base = os.path.basename(path)
        return os.path.splitext(base)[0]

    @classmethod
    def get_map_files(cls, map_config):
        res = []
        for k, v in map_config.items():
            name = k
            if k == 'map':
                name = 'png'
            res.append({
                'type': constants.TOKEN_TYPE.MAP_FILE,
                'local_file': v,
                'content_name': 'map_%s' % name,
                'content_type': constants.MAP_CONTENT_TYPE[k],
                'filename': cls.get_map_file_name(v),
            })
        return res

    @classmethod
    def _handle_upload_map(cls, msg, **kwargs):
        logger.info("RoboxHandler._handle_upload_map", extra={
            "recv_msg": msg,
            "recv_kwargs": kwargs
        })
        arg = msg.get('arg', {})
        map_id = arg.get('map_id', None)
        task_id = map_id
        if map_id is not None:
            map_config = maps.find_map(map_id)
            if map_config:
                cls.send_msg(
                    constants.MESSAGE_ID.ROBOX_STORAGE,
                    constants.MAPPING.MAP_CLOUD_UPLOAD,
                    {
                        'files': cls.get_map_files(map_config),
                        "map_id": map_id,
                        'task_id': task_id
                    }
                )
                return

        ret_msg = {
            'id': msg['id'],
            'act': msg['act'] + '!',
            'ret': {'map_id': map_id},
            'err': {
                'dm': 'invalid',
                'ec': 400,
                'em': 'invalid',
            },
            'ext': msg.get('ext', {}),
            'ebd': msg.get('ebd', {})
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg, **kwargs)

    @classmethod
    def _handle_upload_maps(cls, msg, **kwargs):
        logger.info("RoboxHandler._handle_upload_maps", extra={
            "recv_msg": msg,
            "recv_kwargs": kwargs
        })
        arg = msg.get('arg', {})
        map_ids = arg.get('map_ids', None)
        task_id = hashlib.md5(json.dumps(map_ids).encode('utf-8')).hexdigest()
        if isinstance(map_ids, list):
            message = {
                'map_ids': map_ids,
                'maps_files': []
            }
            for map_id in map_ids:
                map_config = maps.find_map(map_id)
                if map_config:
                    files = cls.get_map_files(map_config)
                    message['maps_files'].append({
                        'map_id': map_id,
                        'files': files
                    })
            message["task_id"] = task_id
            if len(message['maps_files']) == len(map_ids):
                cls.send_msg(
                    constants.MESSAGE_ID.ROBOX_STORAGE,
                    constants.MAPPING.MAPS_CLOUD_UPLOAD,
                    message,
                    map_ids=map_ids,
                )
                return

        ret_msg = {
            'id': msg['id'],
            'act': msg['act'] + '!',
            'ret': {'map_ids': map_ids},
            'err': {
                'dm': 'invalid',
                'ec': 400,
                'em': 'invalid',
            },
            'ext': msg.get('ext', {}),
            'ebd': msg.get('ebd', {})
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg, **kwargs)
