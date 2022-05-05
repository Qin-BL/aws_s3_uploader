import os
import time
import logging
import threading
from robox import config as rconfig

logger = logging.getLogger(__name__)


class FileManagerHandler(object):
    """
    FileManager is a server to manage local files.
    """

    @classmethod
    def initialize(cls, config):
        cls.file_path = config.FILE_MANAGER_PATH
        cls.interval = config.FILE_MANAGER_INTERVAL
        if not os.path.exists(cls.file_path):
            os.makedirs(cls.file_path)

    @classmethod
    def start(cls):
        t = threading.Thread(target=cls.loop, daemon=True)
        t.start()

    @classmethod
    def loop(cls):
        while True:
            try:
                cls.clear()
            except Exception:
                logger.exception(
                    'file.manager.loop.exception', exc_info=True, stack_info=True
                )
            time.sleep(cls.interval)

    @classmethod
    def save(cls, local_file, expire_time=rconfig.FILE_MANAGER_EXPIRE):
        save_path = "%s/%s_%s_%s" % (cls.file_path,
                                            os.path.basename(local_file),
                                            expire_time,
                                            int(time.time()))
        logger.info(
            'file_manager.save',
            extra={'save_path': save_path,
                   'local_file': local_file}
        )
        os.system('ln %s %s' % (local_file, save_path))

    @classmethod
    def clear(cls):
        for file in os.listdir(cls.file_path):
            try:
                now = time.time()
                save_time = int(file.split('_')[-1])
                expire_time = int(file.split('_')[-2])
                if (now - save_time) > expire_time:
                    logger.info(
                        'file_manager.expire',
                        extra={'save_time': save_time,
                               'expire_time': expire_time}
                    )
                    os.remove(os.path.join(cls.file_path, file))
            except:
                logger.exception(
                    'file.manager.clear.exception',
                    extra={
                        'file': file
                    },
                    exc_info=True, stack_info=True
                )

