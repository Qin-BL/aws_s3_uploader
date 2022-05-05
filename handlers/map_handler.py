import glob
import logging
import os
import wget

import constants
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender
from turingvideo.stats import stats as sts
from utils import maps

logger = logging.getLogger(__name__)
stats = sts.get_client(__name__)

MAP_GENERATE_TEMP_DIR = '/etc/zenod/robox/maps'


class RoboxMapHandler(ProcessMessageReceiver, ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX_MAP

    @classmethod
    def initialize(cls):
        cls.register_handlers()
        cls.start_msg_loop()

    @classmethod
    def register_handlers(cls):
        cls.register(constants.MAPPING.MAP_LOCAL_DELETE, cls.handle_map_delete_local)
        cls.register(constants.MAPPING.MAPS_LOCAL_DELETE, cls.handle_maps_delete_local)
        cls.register(constants.ROBOT_APP.MAP_CLOUD_DELETE, cls.handle_map_delete_cloud)
        cls.register(constants.ROBOT_APP.MAPS_CLOUD_DELETE, cls.handle_maps_delete_cloud)
        cls.register(constants.DOWNLOAD_MAP, cls.handle_download_map)


    @classmethod
    def map_generate_intermediate_map(cls, temp_id='*'):
        '''
        get the latest intermediate map or specified by temp_id
        '''
        temp_id = str(temp_id)
        candidates = glob.glob(
            os.path.join(MAP_GENERATE_TEMP_DIR, '%s.jpg' % temp_id)
        )
        candidates = sorted(candidates, reverse=True)
        for map_file in candidates:
            map_basename = os.path.basename(map_file)
            data_filename = map_basename.replace('.jpg', '.yaml')
            data_file_path = os.path.join(MAP_GENERATE_TEMP_DIR, data_filename)
            if not os.path.exists(data_file_path):
                continue
            return {'map': map_file, 'yaml': data_file_path}

        return None

    @classmethod
    def _handle_delete_map(cls, map_id):
        map_config = maps.find_map(map_id)
        if map_config is None:
            logger.info('map %s has been deleted' % map_id)
            return

        def remove_file(file):
            if os.path.exists(file):
                os.remove(file)

        for name, path in map_config.items():
            logger.debug('map.delete.remove', extra={'file-name': name, 'file-path': path})
            remove_file(path)

    @classmethod
    def handle_map_delete_local(cls, msg, **kwargs):
        logger.info('map.delete.local', extra={'payload-msg': msg})
        args = msg.get('arg', {})
        map_id = args.get('map_id', None)
        ret_msg = msg
        ret_msg['act'] += '!'
        ret_msg.pop('arg', None)
        if map_id is None:
            logger.error(
                'map.delete.error',
                extra={
                    'upload-msg': msg,
                }
            )
            ret_msg['err'] = {'dm': 'input', 'ec': 103, 'em': 'map id needed'}
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)
            return

        # Tells cloud to remove file as well.
        cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.MAPPING.MAP_LOCAL_DELETE, args, **kwargs)

        # Deletes local file
        cls._handle_delete_map(map_id)
        ret_msg['err'] = {'ec': 0, 'em': 'ok', 'dm': 'ok'}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_maps_delete_local(cls, msg, **kwargs):
        logger.info('maps.delete.local', extra={'payload-msg': msg})
        args = msg.get('arg', {})
        map_ids = args.get('map_ids', None)
        ret_msg = msg
        ret_msg['act'] += '!'
        ret_msg.pop('arg', None)
        if map_ids is None:
            logger.error(
                'maps.delete.error',
                extra={
                    'upload-msg': msg,
                }
            )
            ret_msg['err'] = {'dm': 'input', 'ec': 103, 'em': 'map ids needed'}
            cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)
            return

        # Tells cloud to remove file as well.
        cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.MAPPING.MAPS_LOCAL_DELETE, args, **kwargs)

        # Deletes local files
        for map_id in map_ids:
            cls._handle_delete_map(map_id)
        ret_msg['err'] = {'ec': 0, 'em': 'ok', 'dm': 'ok'}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_map_delete_cloud(cls, msg, **kwargs):
        logger.info('map.delete.cloud', extra={'payload-msg': msg})
        args = msg.get('arg', {})
        map_id = args.get('map_id', None)

        ret_msg = msg
        ret_msg['act'] += '!'
        ret_msg.pop('arg', None)
        if map_id is None:
            logger.error(
                'map.delete.error',
                extra={
                    'upload-msg': msg,
                }
            )
            ret_msg['err'] = {'dm': 'input', 'ec': 103, 'em': 'map id needed'}
            cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)
            return

        # Cloud has removed the s3, just deletes local copy
        cls._handle_delete_map(map_id)

        ret_msg['err'] = {'ec': 0, 'em': 'ok', 'dm': 'ok'}
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_maps_delete_cloud(cls, msg, **kwargs):
        logger.info('maps.delete.cloud', extra={'payload-msg': msg})
        args = msg.get('arg', {})
        map_ids = args.get('map_ids', [])

        ret_msg = msg
        ret_msg['act'] += '!'
        ret_msg.pop('arg', None)

        for map_id in map_ids:
            # Cloud has removed the s3, just deletes local copy
            cls._handle_delete_map(map_id)

        ret_msg['err'] = {'ec': 0, 'em': 'ok', 'dm': 'ok'}
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg)

    @classmethod
    def handle_download_map(cls, msg, **kwargs):
        logger.info('map.download', extra={'payload-msg': msg})
        arg = msg.get('arg', {})
        map_id = arg.get('map_id', None)
        files = arg.get('files', [])
        ret_msg = {
            'id': msg['id'],
            'act': msg['act'] + '!',
            'err': {
                'dm': 'ok',
                'ec': 0,
                'em': 'ok',
            },
            'ret': {'map_id': map_id},
            'ext': msg.get('ext', {}),
            'ebd': msg.get('ebd', {})
        }

        if map_id and files:
            if maps.find_map(map_id, maps.DEFAULT_MAP_DIR):
                logger.exception(
                    'map.download.error',
                    extra={
                        'map_id': map_id,
                        'err_msg': 'duplicate map'
                    },
                )
                # map already downloaded
                ret_msg['err'] = {
                    'dm': 'duplicate',
                    'ec': 103,
                    'em': 'duplicate',
                }
            else:
                # TODO(nick): convert this to async
                for file in files:
                    try:
                        file = file.get('file')
                        url = file.get('url')

                        # If file already exists, returns error.
                        wget.download(url, out=maps.DEFAULT_MAP_DIR, bar=None)
                    except Exception as e:
                        logger.exception(
                            'robot.download.map.exception',
                            extra={'err-msg': str(e)},
                            exc_info=True,
                            stack_info=True
                        )
                        ret_msg['err'] = {
                            'dm': 'failed',
                            'ec': 103,
                            'em': str(e),
                        }
        else:
            ret_msg['err'] = {
                    'dm': 'invalid',
                    'ec': 103,
                    'em': 'invalid',
            }
        cls.send_msg(constants.MESSAGE_ID.CLOUD_WS, constants.WS_SEND, ret_msg, **kwargs)
