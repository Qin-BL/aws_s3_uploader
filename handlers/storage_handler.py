import collections
import datetime
import logging
import os
import json
import threading
import glob
import time
import sys
from dateutil.tz import tzutc, tzlocal

import constants
from robox import config as rconfig
from configs.manager import RoboxConfigManager
from modules.auth.auth_manager import AuthService
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender
from turingvideo.stats import stats as sts
from modules.utils.datetimes import datetime_str
from modules.utils.lists import list_get_safe
from modules.utils.result import Result, NewResult
from modules.cameras import CAMERA_RECORDING
from modules.utils import timers
from handlers.http_handler import RoboxHTTPHandler
from handlers.file_manager_handler import FileManagerHandler
from robot.inspection_executor import InspectionExecutor
from robot.s3_multipart_uploader import S3MultipartUploader
from robot.cache import cache_installed

logger = logging.getLogger(__name__)
stats = sts.get_client(__name__)


class RoboxStorageHandler(ProcessMessageReceiver, ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX_STORAGE

    @classmethod
    def initialize(cls):
        '''
        cloud_storage_cache: [
          (
            camera_id: <id>
            storage_info: {<s3 upload result>}
            started_at_utc: <local datetime>
            ended_at_utc: <local datetime>
          )
        ]
        event_info_cache: {
          <id>: {
            camera_id: <id>
            deleted: <bool>
            data: [{<s3 upload result>}]
            started_at_utc: <local datetime>
            ended_at_utc: <local datetime> || None
          }
        }
        '''
        cls.cache_lock = threading.Lock()
        cls.cloud_storage_cache = collections.deque(maxlen=15)
        cls.event_info_cache = {}

        cls.register_handlers()
        cls.start_msg_loop()
        cls.inspection_executor = InspectionExecutor(cls.handle_upload_inspections, cls.cleanup_upload_inspections)
        cls.inspection_executor.start()
        cls.s3_multipart_uploader = S3MultipartUploader()
        cls.s3_multipart_uploader.register_callbacks(constants.S3_CALLBACK_TYPES.UPLOAD_MAP,
                                                     {
                                                         constants.S3_CALLBACK_FUNC.ON_SUCCESS: cls.on_map_cloud_upload_finish,
                                                         constants.S3_CALLBACK_FUNC.ON_EXPIRE: cls.on_map_cloud_upload_error
                                                     })
        cls.s3_multipart_uploader.register_callbacks(constants.S3_CALLBACK_TYPES.UPLOAD_MAPS,
                                                     {
                                                         constants.S3_CALLBACK_FUNC.ON_SUCCESS: cls.on_maps_cloud_upload_finish,
                                                     })
        cls.s3_multipart_uploader.start()
        # to ensure thread-safe
        upload_checkers = [
            {
                'upload_type': constants.UPLOAD_TYPE.INSPECTION,
                'upload_path': rconfig.RECORDING_DIR,
                'file_patterns': [
                    CAMERA_RECORDING.INSPECTION_SNAPSHOT_GLOB_PATTERN,
                    CAMERA_RECORDING.INSPECTION_VIDEO_GLOB_PATTERN
                ],
                'camera_id': 'main_camera'
            },
            {
                'upload_type': constants.UPLOAD_TYPE.SEGMENT,
                'upload_path': rconfig.RECORDING_DIR,
                'file_patterns': [
                    CAMERA_RECORDING.SEGMENT_FILE_GLOB_TEMPLATE
                ],
                'camera_id': 'main_camera'
            },
            {
                'upload_type': constants.UPLOAD_TYPE.SNAPSHOT,
                'upload_path': rconfig.RECORDING_DIR,
                'file_patterns': [
                    CAMERA_RECORDING.SNAPSHOT_GLOB_PATTERN
                ],
                'camera_id': 'main_camera'
            },
        ]
        timers.defer_func(
            0,
            cls.check_upload_files_loop,
            upload_checkers,
            rconfig.UPLOAD_FILE_CHECK_INTERVAL,
        )

    @classmethod
    def register_handlers(cls):
        cls.register(constants.EVENT.UPLOAD, cls.initiate_event_upload)
        cls.register(constants.SNAPSHOT.UPLOAD, cls.initiate_snapshot_upload)
        cls.register(
            constants.INSPECTION.UPLOAD,
            cls.initiate_inspection_upload
        )
        cls.register(
            constants.MONIT_EVENT.UPLOAD, cls.initiate_monit_event_upload
        )
        cls.register(constants.MAPPING.MAP_CLOUD_UPLOAD, cls.initiate_map_cloud_upload)
        cls.register(constants.MAPPING.MAPS_CLOUD_UPLOAD, cls.initiate_maps_cloud_upload)
        cls.register(constants.MAPPING.FINISH_S3_TASK, cls.finish_s3_task)

    @classmethod
    def finish_s3_task(cls, msg, **kwargs):
        logger.info("RoboxStorageHandler.finish_s3_task", extra={
            "recv_msg": msg,
            "recv_kwargs": kwargs
        })
        task_id = msg["arg"]["task_id"]
        cls.s3_multipart_uploader.pop_task(task_id)

    @classmethod
    def get_cached_events(cls):
        with cls.cache_lock:
            return cls.event_info_cache.copy()

    @classmethod
    def queue_event_start(cls, event_id, camera_id, start_utc):
        with cls.cache_lock:
            cls.event_info_cache[event_id] = {
                'camera_id': camera_id,
                'deleted': False,
                'started_at_utc': start_utc,
                'ended_at_utc': None,
                'data': None
            }

    @classmethod
    def delete_event(cls, event_id):
        with cls.cache_lock:
            if event_id not in cls.event_info_cache:
                return
            cls.event_info_cache[event_id]['deleted'] = True

    @classmethod
    def pop_event_cache(cls, event_id):
        with cls.cache_lock:
            return cls.event_info_cache.pop(event_id, None)

    @classmethod
    def queue_event_end(cls, event_id, end_utc, event_data):
        with cls.cache_lock:
            if event_id not in cls.event_info_cache:
                logger.error(
                    'event.cache.not-found',
                    extra={
                        'event-id': event_id,
                        'event-data': event_data,
                        'event-end-utc': end_utc
                    }
                )
                return

            cls.event_info_cache[event_id].update(
                {
                    'ended_at_utc': end_utc,
                    'data': event_data
                }
            )

    @classmethod
    def cleanup_upload_local_files(cls, msg):
        upload_configs = msg.get('configs', [])
        for upload_config in upload_configs:
            logger.debug(
                'cleanup.upload.local.files',
                extra={ 'upload_config': upload_config }
            )

            local_file = upload_config.get('local_file', '')
            if local_file and os.path.exists(local_file):
                os.remove(local_file)

            local_meta = upload_config.get('local_meta', '')
            if local_meta and os.path.exists(local_meta):
                os.remove(local_meta)

    @classmethod
    def on_upload_error(cls, err, runtime_info, msg, **kwargs):
        '''Callback when event upload has error'''
        logger.error(
            'storage.upload.error',
            extra={
                'upload-runtime': runtime_info,
                'upload-error': err,
            }
        )
        stats.incr('upload.error', 1)
        cls.cleanup_upload_local_files(msg)

    @classmethod
    def get_upload_configs(cls, file_configs):
        '''
        file_configs: [
          {
            type: source of uplaod files
            content_name: <snapshot | video>
            content_type: <content type>
            local_file: <local absolute path>
          }
        ]
        '''
        configs = []

        for idx, f in enumerate(file_configs):
            ftype = f.get('type', '')
            if not ftype:
                logger.error(
                    'upload.type.not-found',
                    extra={
                        'upload-config': f,
                        'config-index': idx,
                    }
                )
                continue

            token_name = constants.TOKEN_NAME[ftype]
            config = {
                'local_file': f.get('local_file', None),
                'local_meta': f.get('local_meta', None),
                'content_type': f.get('content_type', ''),
                'token_name': token_name,
                'content_name': f.get('content_name', ''),
                'robot_id': AuthService.get_box_name(),
            }
            fname = f.get('filename', None)
            if fname:
                config['filename'] = fname
            configs.append(config)
        return configs

    @classmethod
    def get_map_s3_multipart_upload_configs(cls, file_configs, map_id, callback_type):
        configs = []
        for idx, f in enumerate(file_configs):
            ftype = f.get('type', '')
            if not ftype:
                logger.error(
                    'upload.type.not-found',
                    extra={
                        'upload-config': f,
                        'config-index': idx,
                    }
                )
                continue
            token_name = constants.TOKEN_NAME[ftype]
            local_file = f.get('local_file', None)
            local_file_size = os.path.getsize(local_file)
            local_file_name = os.path.basename(local_file)
            config = {
                'file_type': token_name,
                'callback_type': callback_type,
                'local_file': local_file,
                'local_file_size': local_file_size,
                'local_file_name': local_file_name,
                'local_meta': f.get('local_meta', None),
                'content_type': f.get('content_type', ''),
                'content_name': f.get('content_name', ''),
                'robot_id': AuthService.get_box_name(),
                'map_id': map_id
            }
            configs.append(config)
        return configs

    @classmethod
    def initiate_s3_upload(cls, msg, kwargs):
        logger.info(
            's3.upload.initiate',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
            }
        )

        cls.send_msg(constants.MESSAGE_ID.S3, constants.STORAGE.UPLOAD, msg, **kwargs)
    @classmethod
    def get_map_files_by_map_id(cls, upload_res):
        map_dict = dict()
        for file_res in upload_res:
            map_id = file_res["map_id"]
            map_dict[map_id] = map_dict.get(map_id, [])
            map_dict[map_id].append({
                "file": {
                    "Bucket": file_res["Bucket"],
                    "Key": file_res["Key"],
                    "meta": {
                        "content_type": file_res['content_type'],
                        "file_size": file_res["local_file_size"]
                    }
                },
                "type": file_res["content_name"]
            })
        res = []
        for k,v in map_dict.items():
            res.append({
                "map_id": k,
                "files": v
            })
        return res

    @classmethod
    def on_map_cloud_upload_finish(cls, upload_msg, upload_res):
        """
        'files':[{
            "file": {
              "Bucket": "wanda-poc",
              "Key": "users/8/robots/robot_dog_sr_66015/assets/robot_dog_sr_66040.1637721698311.000000.yaml",
              "meta": {
                "content_type": "text/yaml",
                "file_size": 219
              }
            },
            "type": "map_yaml"
          },...]
        """
        logger.info(
            'map.cloud.upload.finish',
            extra={
                'upload_msg': upload_msg,
                'upload_res': upload_res
            }
        )
        result = NewResult()
        try:
            ret = cls.get_map_files_by_map_id(list(upload_res.values()))[0]
            ret.update(task_id=upload_msg.get('task_id'))
            ret_msg = {
                'act': constants.UPLOAD_MAP + '!',
                'ret': ret,
                'err': {'dm': 'ok', 'ec': 0, 'em': 'ok'},
                'ext': upload_msg.get('ext', {}),
                'ebd': upload_msg.get('ebd', {}),
            }
            logger.info("map.on_map_cloud_upload_finish", extra={"ret_msg": ret_msg})
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)
            result.dm = 'waiting'
        except Exception as e:
            result.dm = "callback_error"
            result.em = str(e)
            result.status_code = 500
            logger.error("map.cloud.upload.finish.error", extra={
                'upload_msg': upload_msg,
                'upload_res': upload_res
            }, exc_info=True, stack_info=True)
        return result

    @classmethod
    def on_map_cloud_upload_error(cls, upload_msg):
        logger.error(
            'map.cloud.upload.error',
            extra={
                'upload-msg': upload_msg,
            }
        )
        result = NewResult()
        try:
            map_id = upload_msg.get('map_id', None)
            ret_msg = {
                'act': constants.UPLOAD_MAP + '!',
                'ret': {
                    'map_id': map_id,
                    'files': []
                },
                'err': {'dm': 'operation_failed', 'ec': -1, 'em': 'upload_failed'}
            }
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)
        except Exception as e:
            result.dm = "callback_error"
            result.em = str(e)
            result.status_code = 500
            logger.error("map.cloud.upload.error.error", extra={
                'upload_msg': upload_msg,
            }, exc_info=True, stack_info=True)
        return result

    @classmethod
    def initiate_map_cloud_upload(cls, msg, **kwargs):
        logger.info(
            'map.cloud.upload.init',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
            }
        )
        file_configs = msg.get('files', [])
        task_id = msg.get('task_id')
        map_id = msg.get('map_id')
        ebd = msg.get('ebd', None)
        now = time.time()
        upload_msg = {
            'files_config': cls.get_map_s3_multipart_upload_configs(file_configs, map_id,
                                                                    constants.S3_CALLBACK_TYPES.UPLOAD_MAP),
            'task_id': task_id,
            'expire_time': now + rconfig.DEFAULT_UPLOAD_EXPIRE_TIME,
            'ebd': ebd
        }

        enqueue_res = cls.s3_multipart_uploader.enqueue(upload_msg, tag=map_id)
        ret_msg = {
            'act': constants.UPLOAD_MAP + '!',
            'ret': {
                'map_id': map_id,
                'files': []
            },
            "err": enqueue_res.raw["err"]
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def on_maps_cloud_upload_finish(cls, upload_msg, upload_res):
        """
        upload_res:
        [{
            "map_id": <map id>,
            'local_file': <file path>,
            'local_file_size': <file size>,
            'local_file_name': <file name>,
            'file_type': <constants.TOKEN_NAME>,
            "Bucket": upload_bucket,
            "Key": upload_key,
        },...]
        """
        logger.info(
            'maps.cloud.upload.finish',
            extra={
                'upload-msg': upload_msg,
                'upload-res': upload_res
            }
        )
        result = NewResult()
        try:
            map_files = cls.get_map_files_by_map_id(list(upload_res.values()))
            map_ids = [i["map_id"] for i in map_files]
            ret_msg = {
                'act': constants.UPLOAD_MAPS + '!',
                'ret': {"map_ids": map_ids, "results": map_files, 'task_id': upload_msg["task_id"]},
                'err': {'dm': 'ok', 'ec': 0, 'em': 'ok'},
                'ext': upload_msg.get('ext', {}),
                'ebd': upload_msg.get('ebd', {}),
            }
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)
            result.dm = 'waiting'
        except Exception as e:
            result.dm = "callback_error"
            result.em = str(e)
            result.status_code = 500
            logger.error("maps.cloud.upload.finish.error", extra={
                'upload_msg': upload_msg,
            }, exc_info=True, stack_info=True)
        return result

    @classmethod
    def initiate_maps_cloud_upload(cls, msg, **kwargs):
        """
        :param msg:
        {
            'task_id': uuid
            'map_ids': [],
            'maps_files': [
                {
                    'map_id': map_id,
                    'files': []
                }
            ]
        }
        :param kwargs:
        :return:
        """
        logger.info(
            'maps.cloud.upload.init',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
            }
        )
        now = time.time()
        enqueue_res = NewResult()
        for map_files in msg["maps_files"]:
            map_id = map_files['map_id']
            files_config = cls.get_map_s3_multipart_upload_configs(map_files["files"], map_id,
                                                                    constants.S3_CALLBACK_TYPES.UPLOAD_MAPS)
            upload_msg = {
                'files_config': files_config,
                'task_id': msg["task_id"]+map_id,
                'expire_time': now + rconfig.DEFAULT_UPLOAD_EXPIRE_TIME,
            }
            enqueue_res = cls.s3_multipart_uploader.enqueue(upload_msg, tag=map_id)
            if not enqueue_res:
                logger.error("maps.cloud.upload.init.enqueue.error",
                             extra={
                                 'upload-msg': msg,
                                 'upload-kwargs': kwargs,
                                 'enqueue_res': enqueue_res
                             })
                break
        ret_msg = {
            'act': constants.UPLOAD_MAPS + '!',
            'ret': {
                'map_ids': msg["map_ids"],
                'results': [],
            },
            'err': enqueue_res.raw["err"]
        }
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def queue_upload_map(cls, map_files, on_success, on_error):
        logger.info(
            'maps.cloud.upload.queue',
            extra={
                'upload-files': map_files,
            }
        )
        map_id = map_files.get('map_id')
        file_configs = map_files.get('files', [])
        upload_msg = {
            'configs': cls.get_upload_configs(file_configs)
        }
        upload_kwargs = {
            'on_success': on_success,
            'on_error': on_error,
            'map_id': map_id
        }

        cls.initiate_s3_upload(upload_msg, upload_kwargs)

    @classmethod
    def on_snapshot_upload_finish(cls, upload_results, msg, **kwargs):
        logger.info(
            'snapshot.upload.finish',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
                'upload-results': upload_results,
            }
        )
        stats.incr('snapshot.success', 1)

        utc_datetime = kwargs.get('utc_datetime')
        location = kwargs.get('location', None)  # Optional
        camera_id = kwargs.get('camera_id')

        all_medium = []
        all_configs = msg.get('configs', [])

        for idx, result in upload_results.items():
            medium = {
                'file': result,
            }
            upload_config = list_get_safe(all_configs, idx, {})
            name = upload_config.get('content_name', '')
            if name:
                medium['type'] = name
            medium['timestamp'
                   ] = datetime_str(utc_datetime.astimezone(tz=tzutc()))
            all_medium.append(medium)

        upload_data = all_medium[0]
        if location:
            upload_data['location'] = location
        upload_data['camera'] = 'front'

        cls.send_msg(
            constants.MESSAGE_ID.ROBOX_HTTP,
            constants.SNAPSHOT.CLOUD_CREATE,
            upload_data,
        )

        cls.cleanup_upload_local_files(msg)

    @classmethod
    def initiate_snapshot_upload(cls, msg, **kwargs):
        '''
        kwargs: {
          'utc_datetime'
        }
        '''
        file_configs = msg.get('files', [])

        upload_msg = {}

        upload_msg['configs'] = cls.get_upload_configs(file_configs)

        upload_kwargs = kwargs
        upload_kwargs['on_success'] = cls.on_snapshot_upload_finish
        upload_kwargs['on_error'] = cls.on_upload_error

        stats.incr('snapshot.initiate', 1)
        cls.initiate_s3_upload(upload_msg, upload_kwargs)

    @classmethod
    def on_event_upload_error(cls, err, runtime_info, msg, **kwargs):
        event_id = kwargs.get('event_id', None)
        cls.delete_event(event_id)
        cls.on_upload_error(err, runtime_info, msg, **kwargs)

    @classmethod
    def on_event_upload_finish(cls, upload_results, msg, **kwargs):
        '''Callback when event upload is complete
        Note: data should be validated before upload,
        so we assume valid data here.
        '''
        logger.info(
            'event.upload.finish',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
                'upload-results': upload_results,
            }
        )

        detection_types = kwargs.get('detection-types')
        utc_start = kwargs.get('utc_start')
        utc_end = kwargs.get('utc_end')
        location = kwargs.get('location', None)  # Optional
        camera_id = kwargs.get('camera_id', 'main_camera')
        event_id = kwargs.get('event_id', '')
        threshold = kwargs.get('detection-threshold', 0)

        all_configs = msg.get('configs', [])
        all_medium = {'videos': []}
        for idx, result in upload_results.items():
            medium = {
                'file': result,
            }
            upload_config = list_get_safe(all_configs, idx, {})
            name = upload_config.get('content_name', '')
            if not name:
                logger.error(
                    'storage.unknow_content',
                    extra={
                        'upload-config': upload_config,
                        'config-idx': idx,
                    }
                )
                continue
            all_medium[name] = medium

        all_medium['bbox'] = {
            'file': {
                'Key': 'bbox',
                'Bucket': 'bbox-bucket',
                'meta': {
                    'content_type': 'text/json',
                    'file_size': 0
                },
            }
        }

        upload_data = {
            'started_at': datetime_str(utc_start.astimezone(tz=tzutc())),
            'ended_at': datetime_str(utc_end.astimezone(tz=tzutc())),
            'media': all_medium,
            'detection': {
                'camera': 'front',
                'algos': ','.join(list(detection_types)),
                'threshold': threshold,
            }
        }
        if location:
            upload_data['location'] = location

        cls.queue_event_end(event_id, utc_end, upload_data)
        cls.cleanup_upload_local_files(msg)

    @classmethod
    def initiate_event_upload(cls, msg, **kwargs):
        '''
        kwargs: {
          detection_types: [<type>],
          utc_start: <utc datetime>,
          utc_end: <utc datetime>,
          event_id: <id string>,
        }
        '''
        file_configs = msg.get('files', [])
        upload_msg = {}

        detection_types = kwargs.get('detection-types', [])
        if len(detection_types) < 1:
            logger.warn(
                'event.upload.skip',
                extra={
                    'upload-msg': msg,
                    'upload-kwargs': kwargs,
                }
            )
            return

        upload_msg['configs'] = cls.get_upload_configs(file_configs)

        upload_kwargs = kwargs
        upload_kwargs['on_success'] = cls.on_event_upload_finish
        upload_kwargs['on_error'] = cls.on_event_upload_error

        stats.incr('event.initiate', 1)
        cls.initiate_s3_upload(upload_msg, upload_kwargs)

    @classmethod
    def on_monit_event_upload_success(cls, upload_results, msg, **kwargs):
        logger.info(
            'monit-event.upload.finish',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
                'upload-results': upload_results,
            }
        )

        detected_macs = kwargs.get('detected_macs')
        utc_start = kwargs.get('utc_start')
        utc_end = kwargs.get('utc_end')

        all_configs = msg.get('configs', [])
        all_medium = []
        for idx, result in upload_results.items():
            medium = {
                'file': result,
            }
            upload_config = list_get_safe(all_configs, idx, {})
            name = upload_config.get('content_name', '')
            if name:
                medium['name'] = name
            all_medium.append(medium)

        all_medium.append(
            {
                'name': 'bbox',
                'file': {
                    'Key': 'bbox',
                    'Bucket': 'bbox-bucket',
                    'meta': {
                        'content_type': 'text/json',
                        'file_size': 0
                    },
                }
            }
        )

        upload_data = {
            'started_at': datetime_str(utc_start.astimezone(tz=tzutc())),
            'ended_at': datetime_str(utc_end.astimezone(tz=tzutc())),
            'mediums': all_medium,
            'detected_macs': detected_macs
        }

        # TODO(gh): location is optional

        stats.incr('monit-event.success', 1)
        cls.send_msg(
            constants.MESSAGE_ID.ROBOX_HTTP, constants.MONIT_EVENT.CLOUD_CREATE, upload_data
        )

        cls.cleanup_upload_local_files(msg)

    @classmethod
    def initiate_monit_event_upload(cls, msg, **kwargs):
        '''
        kwargs: {
          detected_macs: [{
            mac_address: <mac string>,
            event_type: 'enter' | 'leave'
          }],
          utc_start: <utc datetime>,
          utc_end: <utc datetime>
        }
        '''
        detected_macs = kwargs.get('detected_macs', [])

        if len(detected_macs) < 1:
            logger.error(
                'monit_event.upload.skip',
                extra={
                    'upload-msg': msg,
                    'upload-kwargs': kwargs,
                }
            )
            return

        file_configs = msg.get('files', [])
        upload_msg = {}

        upload_msg['configs'] = cls.get_upload_configs(file_configs)
        upload_kwargs = kwargs
        upload_kwargs['on_success'] = cls.on_monit_event_upload_success
        upload_kwargs['on_error'] = cls.on_upload_error

        stats.incr('monit-event.initiate', 1)
        cls.initiate_s3_upload(upload_msg, upload_kwargs)

    @classmethod
    def on_segment_upload_finish(cls, upload_results, msg, **kwargs):
        '''Callback when event upload is complete
        Note: data should be validated before upload,
        so we assume valid data here.
        '''
        logger.info(
            'video.segment.upload.finish',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
                'upload-results': upload_results,
            }
        )

        started_at_utc = kwargs.get('started_at')
        ended_at_utc = kwargs.get('ended_at')
        camera_id = kwargs.get('camera_id')
        upload_enabled = kwargs.get('enable_upload', False)

        video_upload_result = list_get_safe(
            list(upload_results.values()), 0, None
        )
        if not video_upload_result:
            cls.cleanup_upload_local_files(msg)
            return

        upload_data = {
            'started_at': datetime_str(started_at_utc),
            'ended_at': datetime_str(ended_at_utc),
            'camera': 'front',
            'video': video_upload_result,
        }

        stats.incr('segment.success', 1)
        if upload_enabled:
            cls.send_msg(
                constants.MESSAGE_ID.ROBOX_HTTP, constants.VIDEO_SEGMENT.CLOUD_CREATE, upload_data
            )

        cls.cleanup_upload_local_files(msg)
        cls.segment_append_event_data(
            camera_id, started_at_utc, ended_at_utc, video_upload_result
        )

    @classmethod
    def segment_append_event_data(cls, camera_id, utc_start, utc_end, upload_result):
        cls.cloud_storage_cache.append((camera_id, utc_start, utc_end, upload_result))
        desc_cache = sorted(
            list(cls.cloud_storage_cache), reverse=True, key=lambda c: c[1]
        )

        all_events = cls.get_cached_events()
        for event_id, head in all_events.items():
            camera = head['camera_id']
            if head is None or head['deleted']:
                cls.pop_event_cache(event_id)
                continue

            event_start_utc = head['started_at_utc']
            event_end_utc = head.get('ended_at_utc', None)
            if event_end_utc is None and utc_start - event_start_utc > datetime.timedelta(
                minutes=3
            ):
                logger.error(
                    'event.cache.stuck',
                    extra={
                        'event-id': event_id,
                        'event-cache': head,
                        'current-segment': (utc_start, utc_end),
                    }
                )
                cls.pop_event_cache(event_id)
                continue

            if (camera != camera_id or
                event_end_utc is None or event_end_utc > utc_end):
                continue

            cls.pop_event_cache(event_id)
            videos = []
            for cache_segment in desc_cache:
                cam, seg_start_utc, seg_end_utc, cache_result = cache_segment
                if cam != camera or seg_start_utc > event_end_utc:
                    continue
                if seg_end_utc < event_start_utc:
                    break
                videos.insert(
                    0, {
                        'file': cache_result,
                        'started_at': datetime_str(seg_start_utc),
                        'ended_at': datetime_str(seg_end_utc),
                    }
                )

            if len(videos) < 1:
                continue

            event_data = head['data']
            event_media = event_data['media']
            event_media['videos'] = videos

            event_data['media'] = event_media
            cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.EVENT.CLOUD_CREATE, event_data)

    @classmethod
    def initiate_video_segment_upload(
        cls, camera_id, segment_file, start_dt, end_dt
    ):
        FileManagerHandler.save(segment_file)
        robot_config = RoboxConfigManager.get_robot_config()

        enable_video_segment = robot_config.get('upload_video_enabled', False)

        if not enable_video_segment:
            relevant_events = [
                key for key in cls.event_info_cache.keys()
                if cls.event_info_cache[key]['camera_id'] == camera_id
            ]
            if len(relevant_events) < 1:
                try:
                    os.remove(segment_file)
                except Exception:
                    pass
                return

        file_config = [
            {
                'type': constants.TOKEN_TYPE.VIDEO_SEGMENT,
                'content_name': 'video',
                'content_type': constants.CONTENT_TYPE.VIDEO,
                'local_file': segment_file,
            }
        ]

        upload_msg = {'configs': cls.get_upload_configs(file_config)}
        upload_kwargs = {
            'started_at': start_dt.astimezone(tz=tzutc()),
            'ended_at': end_dt.astimezone(tz=tzutc()),
            'camera_id': camera_id,
            'enable_upload': enable_video_segment,
            'on_success': cls.on_segment_upload_finish,
            'on_error': cls.on_upload_error,
        }

        stats.incr('segment.initiate', 1)
        cls.initiate_s3_upload(upload_msg, upload_kwargs)

    @classmethod
    def on_inspection_upload_finish(cls, upload_results, msg, **kwargs):
        logger.info(
            'inspection.upload.finish',
            extra={
                'upload-msg': msg,
                'upload-kwargs': kwargs,
                'upload-results': upload_results,
            }
        )

        all_configs = msg.get('configs', [])
        all_medium = []
        for idx, result in upload_results.items():
            medium = {
                'file': result,
            }
            upload_config = list_get_safe(all_configs, idx, {})
            name = upload_config.get('content_name', '')
            if name:
                medium['name'] = name
            all_medium.append(medium)

        utc_start = kwargs.get('started_at')
        utc_end = kwargs.get('ended_at')
        location = kwargs.get('location', None)  # Optional
        way_point = kwargs.get('way_point', None)  # Optional
        camera = kwargs.get('camera')
        routine_id = kwargs.get('routine_id', None)
        routine_execution_id = kwargs.get('routine_execution_id', None)
        inspection_type = kwargs.get('type')
        tmp_uuid = kwargs.get('uuid')

        upload_data = {
            'started_at': utc_start,
            'ended_at': utc_end,
            'mediums': all_medium,
            'camera': camera,
            'type': inspection_type,
            'routine_id': routine_id,
            'routine_execution_id': routine_execution_id,
            'uuid': tmp_uuid
        }
        if location:
            upload_data['location'] = location
        if way_point:
            upload_data['way_point'] = way_point

        stats.incr('inspection.success', 1)
        # cls.send_msg(
        #     constants.MESSAGE_ID.ROBOX_HTTP, constants.INSPECTION.CLOUD_CREATE, upload_data
        # )
        RoboxHTTPHandler.enqueue_create_inspection(upload_data)
        cls.inspection_executor.on_handled(msg['cache_key'], True)
        cls.cleanup_upload_local_files(msg)

    @classmethod
    def on_inspection_upload_error(cls, err, runtime_info, msg, **kwargs):
        """Callback when uploading inspection errors"""
        logger.error(
            'storage.upload.inspection.error',
            extra={
                'runtime_info': runtime_info,
                'upload-msg': msg,
                'upload-error': err
            }
        )
        stats.incr('upload.error', 1)
        cls.inspection_executor.on_handled(msg['cache_key'], False)

    @classmethod
    def initiate_inspection_upload(cls, msg, **kwargs):
        '''
        kwargs: {
          camera_id: <string>  # camera name
          utc_start, utc_end: <UTC datetime>,
          location: (lat, lng)
        }
        '''
        result = Result()
        file_configs = msg.get('files', [])
        if len(file_configs) < 1:
            logger.error(
                'inspection.video.config.empty',
                extra={
                    'upload-msg': msg,
                    'upload-kwargs': kwargs,
                }
            )
            result.status_code = 400
            result.dm = 'config_empty'
            result.em = 'config empty'
            return result

        utc_start = kwargs.get('utc_start')
        utc_end = kwargs.get('utc_end')
        location = kwargs.get('location', None)
        way_point = kwargs.get('way_point', None)
        routine_id = kwargs.get('routine_id', None)
        routine_execution_id = kwargs.get('routine_execution_id', None)
        inspection_type = kwargs.get('type', 'scan')
        inspection_uuid = kwargs.get("inspection_uuid")

        if way_point:
            way_point = json.loads(way_point)

        upload_msg = {
            'configs': cls.get_upload_configs(file_configs),
        }
        upload_kwargs = {
            'on_success': cls.on_inspection_upload_finish,
            'on_error': cls.on_inspection_upload_error,
            'started_at': datetime_str(utc_start.astimezone(tz=tzutc())),
            'ended_at': datetime_str(utc_end.astimezone(tz=tzutc())),
            'camera': 'front',
            'type': inspection_type,
            'upload_type': constants.UPLOAD_TYPE.INSPECTION,
            'routine_id': routine_id,
            'routine_execution_id': routine_execution_id,
            'uuid': inspection_uuid
        }
        if location:
            upload_kwargs['location'] = {
                'lat': location[0],
                'lng': location[1],
            }
        if way_point:
            upload_kwargs['way_point'] = way_point

        stats.incr('inspection.initiate', 1)
        return cls.inspection_executor.enqueue(dict(msg=upload_msg, kwargs=upload_kwargs), tag=inspection_uuid)

    @classmethod
    def handle_upload_inspections(cls, key, item):
        item['msg']['cache_key'] = key
        cls.initiate_s3_upload(item['msg'], item['kwargs'])
        return True

    @classmethod
    def cleanup_upload_inspections(cls, item):
        """Callback when uploading expires"""
        logger.error(
            'storage.upload.expires',
            extra={
                'item': item,
                'upload-error': 'expires',
            }
        )
        stats.incr('upload.error', 1)
        cls.cleanup_upload_local_files(item['msg'])

    @classmethod
    def check_upload_files(cls, checkers):
        '''Check the number of files still exist.

        For current uploading pipleine, the file should be delete after a
        success upload. So, we check the number of files in a folder to
        define remaining files waiting to be uploaded.

        Args:
            checkers (list(dict)): A list of checkers. Each checker is defined
            with upload_type, upload_path, camera_id, and pattern. For an
            example:
            checker = {
                'upload_type': 'inspection',
                'upload_path': '/path/to/where/you/save/the/files',
                'file_patterns': ['inspection__{camera_id}__????-??-??_??-??-??.jpg'],
                'camera_id': 'main_camera'
            }

        Returns:
            dict: A list of matching file pathes for each checker. For example:
            {
                'inspection': [
                    '/absolute/path/to/matching/file'
                ]
            }
        '''
        collectors = {}
        for checker in checkers:
            matches = []
            for file_pattern in checker['file_patterns']:
                file_pattern = file_pattern.format(camera_id=checker['camera_id'])
                pattern = os.path.join(checker['upload_path'], file_pattern)
                matches += (glob.glob(pattern))
            upload_type = checker['upload_type']
            if upload_type not in collectors:
                collectors[upload_type] = []
            collectors[upload_type] += matches
        for upload_type in collectors:
            collectors[upload_type] = list(set(collectors[upload_type]))
        return collectors

    @classmethod
    def resume_upload_inspection(cls, local_files):
        '''Resume upload inspection.

        To resume upload an inspection, file and meta data are necessary. Here
        assumes they are stored in that directoy. And, the filename of meta
        should be the same except for the extension.

        Args:
            local_files (list(str)): list of absolute filenames.

        Returns:
            no return values

        '''
        logger.info(
            'resume.upload.inspection.start',
            extra={ 'local-files': local_files }
        )

        for local_file in local_files:
            local_meta = local_file.split('.')[0] + '.json'
            if not os.path.exists(local_file):
                message = 'file does not exist: {0}'.format(local_file)
                logger.error(
                    'resume.upload.inspection.error.file',
                    extra= {'error-msg': message }
                )
            elif not os.path.exists(local_meta):
                message = 'file does not exist: {0}'.format(local_meta)
                logger.error(
                    'resume.upload.inspection.error.meta',
                    extra= {'error-msg': message }
                )

                logger.info(
                    'resume.upload.inspection.remove.file',
                    extra= {'local-file': local_file }
                )
                os.remove(local_file)
            else:
                with open(local_meta) as meta_file:
                    try:
                        meta_data = json.load(meta_file)
                        msg = meta_data['msg']
                        kwargs = meta_data['kwargs']
                        kwargs['utc_start'] = datetime.datetime.fromtimestamp(
                            kwargs['utc_start'],
                            tz=tzlocal()
                        )
                        kwargs['utc_end'] = datetime.datetime.fromtimestamp(
                            kwargs['utc_end'],
                            tz=tzlocal()
                        )

                        logger.debug(
                            'resume.upload.inspection.send',
                            extra={
                                'upload-msg': msg,
                                'upload-kwargs': kwargs
                            }
                        )

                        cls.initiate_inspection_upload(msg, **kwargs)
                    except:
                        e = sys.exc_info()[0]
                        logger.exception(
                            'resume.upload.inspection.exception',
                            extra= { 'error-msg': str(e) },
                            exc_info=True,
                            stack_info=True
                        )

        logger.info('resume.upload.inspection.end')

    @classmethod
    def check_upload_files_loop(cls, checkers, interval):
        '''Keep checking the number of files still exist.

        This is a wrapper of check_upload_tasks by keeping calling
        check_upload_tasks and sending the number of remaining files waiting to
        be upload with repect to upload type to robot.

        Args:
            checkers (list(dict)): For more detial, pleace refer to
            check_upload_files.

        Returns:
            no return values.
        '''
        collectors = cls.check_upload_files(checkers)
        if not cache_installed:
            for upload_type in collectors:
                if upload_type == constants.UPLOAD_TYPE.INSPECTION:
                    cls.resume_upload_inspection(collectors[upload_type])

        logger.info(
            'check.upload.files.loop.start',
            extra={ 'checkers': checkers, 'interval': interval }
        )

        while True:
            time.sleep(interval)

            collectors = cls.check_upload_files(checkers)
            counters = {}
            for upload_type in collectors:
                counters[upload_type] = len(collectors[upload_type])
            counters[constants.UPLOAD_TYPE.INSPECTION] = cls.inspection_executor.size()
            payload = {
                'act': constants.REPORT_UPLOAD_STATUS,
                'arg': { 'queue_size': counters },
                'ret': {}
            }
            logger.debug(
                'check.upload.files.loop.payload',
                extra={ 'payload': payload }
            )
            cls.send_msg(
                constants.MESSAGE_ID.ROBOT_WS,
                constants.WS_SEND,
                payload
            )
