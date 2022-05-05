import logging
import os
import time
import copy
import json
import sys

import constants
from configs import manager
from handlers.ws_handler import RoboxHandler
from modules.cameras.turing_streamer import TuringStreamer
from modules.cameras.ricoh_theta_s_camera import RicohThetaSCamera
from modules.cameras.utils import get_camera_snapshot
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender
from turingvideo.stats import stats as sts
from modules.utils import datetimes
from robox import config
from stats.control_event_log_helper import log_robox_process_from_kwargs

logger = logging.getLogger(__name__)
stats = sts.get_client(__name__)


class RoboxCameraHandler(ProcessMessageReceiver, ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX_CAMERA

    @classmethod
    def initialize(cls, host):
        cls.main_camera_streamer = TuringStreamer(
            hw_accel_enabled=config.FFMPEG_HWACCL
        )
        main_config = manager.RoboxConfigManager.get_camera(camera_id='main_camera')
        cls.main_camera_streamer.set_input_uri(main_config.get(manager.CAMERA_CONFIG_KEY_STREAMING_URI, None))

        cls.usb_camera = RicohThetaSCamera()
        # TODO load dynamic configs for usb camera
        cls.usb_camera.start()

        cls.register_handlers()
        cls.start_msg_loop()

        cls.last_snapshot_upload = 0
        cls.last_start_stream_msg = 0

        cls.last_main_stream_token = None
        # initialize main-camera.sdp
        path = os.path.join(config.BASE, 'static/main-camera.sdp')
        cmd = "sed -i 's/{host}/%s/g' \"%s\"" % (host, path)
        os.system(cmd)

    @classmethod
    def terminate(cls):
        cls.stop_msg_loop()
        cls.main_camera_streamer.stop_stream_safe()
        cls.usb_camera.stop()

    @classmethod
    def register_handlers(cls):
        cls.register(constants.SNAPSHOT.GRAB, cls.handle_snapshot)
        cls.register(constants.SNAPSHOT.KEEP, cls.handle_snapshot_keep)
        cls.register(constants.STREAM.START, cls.handle_start_stream)
        cls.register(constants.STREAM.STOP, cls.handle_stop_stream)

    @classmethod
    @log_robox_process_from_kwargs(should_finalize=True)
    def handle_start_stream(cls, msg, **kwargs):
        stats.incr('start')
        if not RoboxHandler.last_robot_ws_connected:
            ret_msg = msg
            ret_msg['act'] += '!'
            ret_msg['ret'] = {}
            ret_msg['err'] = {'dm': 'robot_offline'}

            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg, **kwargs)
            stats.incr('start-failure')
            return

        args = msg.get('arg', {})
        camera_name = args.get('camera')
        camera_id = 'main_camera'
        cam_config = manager.RoboxConfigManager.get_camera(camera_id=camera_id)
        logger.debug(
            'camera.start_stream.msg',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
                'camera-config': cam_config,
                'camera-name': camera_name,
                'camera-id': camera_id,
            }
        )
        token = args.get('token', {})
        token_url = token.get('base_uri', None)
        stream_url = token.get('uri', None)

        if stream_url is None:
            logger.error(
                'camera.stream_url.empty',
                extra={
                    'token-data': token,
                    'msg-payload': msg,
                    'msg-kwargs': kwargs
                }
            )
            stats.incr('start-failure')
            return

        streamer = cls.main_camera_streamer
        last_token = cls.last_main_stream_token

        success = True
        if token_url != last_token:
            streamer.stop_stream_safe()
            success = streamer.start_stream_safe(stream_url)

            # Token updated
            cls.last_main_stream_token = token_url
        else:
            if not streamer.stream_status_safe():
                streamer.stop_stream_safe()
                success = streamer.start_stream_safe(stream_url)

        ret_msg = msg
        ret_msg['act'] += '!'
        ret_msg['ret'] = {'base_uri': token_url}
        if success:
            ret_msg['err'] = {'dm': 'ok', 'ec': 0, 'em': 'ok'}
        else:
            ret_msg['err'] = {'dm': 'ffmpeg_failed'}

        stats.incr('start-success') if success else stats.incr('start-failure')
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg, **kwargs)

    @classmethod
    @log_robox_process_from_kwargs(should_finalize=True)
    def handle_stop_stream(cls, msg, **kwargs):
        stats.incr('stop')
        cls.main_camera_streamer.stop_stream_safe()

        msg['act'] += '!'
        msg['ret'] = {}
        msg['err'] = {'dm': 'ok'}
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, msg, **kwargs)

    @classmethod
    def _handle_snapshot(cls, camera_id, now, main_camera=False, **kwargs):
        camera_config = manager.RoboxConfigManager.get_camera(camera_id=camera_id)
        uri = camera_config.get(manager.CAMERA_CONFIG_KEY_ROBOT_PUSH_URI, None)
        if not uri:
            return
        output_file = get_camera_snapshot(config.CLIP_DIR, camera_id)

        if not output_file:
            logger.error(
                'camera.snapshot.failed',
                extra={
                    'camera-id': camera_id,
                    'camera-uri': uri,
                }
            )
            return

        if now - cls.last_snapshot_upload < config.SNAPSHOT_INTERVAL:
            os.remove(output_file)
            return

        if main_camera:
            cls.last_snapshot_upload = now

        utc_now = datetimes.get_utc_now()
        upload_msg = {
            'files': [
                {
                    'type': constants.TOKEN_TYPE.SNAPSHOT,
                    'content_name': 'view',
                    'content_type': constants.CONTENT_TYPE.SNAPSHOT,
                    'local_file': output_file,
                    'camera_id': camera_id,
                }
            ]
        }
        upload_kwargs = {
            'utc_datetime': utc_now,
            'camera_id': camera_id,
        }

        cls.send_msg(
            constants.MESSAGE_ID.ROBOX_STORAGE, constants.SNAPSHOT.UPLOAD, upload_msg,
            **upload_kwargs
        )

    @classmethod
    def handle_snapshot(cls, msg, **kwargs):
        now = time.time()
        cls._handle_snapshot('main_camera', now, main_camera=True, **kwargs)

    @classmethod
    def handle_snapshot_keep(cls, msg, **kwargs):
        """ Invoke snapshot from usb camera and keep the file to upload with conditions."""

        if cls.usb_camera.is_opened():
            new_files = []
            for content in msg['files']:
                if content['content_type'] == constants.CONTENT_TYPE.SNAPSHOT:
                    local_file_usb = cls._insert_before_substr(content['local_file'], '.jpg', '__usb')
                    photo_path = cls.usb_camera.take_snapshot(local_file_usb)
                    # if photo generate successfully
                    if photo_path:
                        new_files.append({
                            'type': content.get('type'),
                            'content_type': content.get('content_type'),
                            'local_file': local_file_usb,
                            'local_meta': content.get('local_meta'),
                            'content_name': content.get('content_name'),
                        })
                        logger.info('camera.snapshot.keep')

            if len(new_files) > 0:
                msg['files'] += new_files
                # update meta file with the new files information
                meta_path = new_files[0]['local_meta']
                with open(meta_path, 'r+') as meta_file:
                    try:
                        # in json file, utc_start and utc_end are in float type
                        # in kwargs, utc_start and utc_end are kept in datetime objects
                        # to not change anything, we need to load them from json first
                        meta_data = json.load(meta_file)
                        start_time = meta_data['kwargs']['utc_start']
                        end_time = meta_data['kwargs']['utc_end']
                        # update meta_data with kwargs
                        meta_data = {
                            'msg': copy.deepcopy(msg),
                            'kwargs': copy.deepcopy(kwargs)
                        }

                        # write back utc_start and utc_end in float type
                        meta_data['kwargs']['utc_start'] = start_time
                        meta_data['kwargs']['utc_end'] = end_time

                        # rewrite meta file with updated information
                        meta_file.seek(0)
                        json.dump(meta_data, meta_file)
                        meta_file.truncate()
                    except:
                        e = sys.exc_info()[0]
                        logger.exception(
                            'camera.snapshot.keep.exception',
                            extra={'error-msg': str(e)},
                            exc_info=True,
                            stack_info=True
                        )

        cls.send_msg(
            constants.MESSAGE_ID.ROBOX_STORAGE,
            constants.INSPECTION.UPLOAD,
            msg,
            **kwargs
        )

    @classmethod
    def _insert_before_substr(cls, source_str: str, sub_str: str, prepend_str: str):
        if sub_str not in source_str:
            return source_str
        pos = source_str.index(sub_str)
        return source_str[:pos] + prepend_str + source_str[pos:]

    @classmethod
    def main_streamer_alive(cls):
        return cls.main_camera_streamer.stream_status_safe()
