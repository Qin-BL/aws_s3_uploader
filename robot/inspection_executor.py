import time, os, logging, uuid
from diskcache import Index
from robot.cache import INDEX_DIR_BASE
from robot.cache import INSPECTIONS_FILES_CATEGORY, INSPECTIONS_FILES_EXPIRATION, INSPECTIONS_FILES_CHECK_INTERVAL, \
    INSPECTIONS_UPLOADING_TIMEOUT
from robot.cache_executor import CacheExecutor
from modules.utils.result import NewResult
logger = logging.getLogger(__name__)


class InspectionExecutor(CacheExecutor):
    def __init__(self, handle, cleanup):
        super().__init__(INSPECTIONS_FILES_CATEGORY, INSPECTIONS_FILES_EXPIRATION, INSPECTIONS_FILES_CHECK_INTERVAL, handle)
        self.cleanup = cleanup
        self.index = Index(os.path.join(INDEX_DIR_BASE, INSPECTIONS_FILES_CATEGORY))  # uploading
        self.uploading_timeout = INSPECTIONS_UPLOADING_TIMEOUT

    def _decide_reenqueue(self, index_key, force=False):
        index_value = self.index.get(index_key)
        if index_value is None:
            return
        now = time.time()
        expire_time = index_value.get("expire_time")
        if expire_time is None:
            return
        expire = expire_time - now
        if expire <= 0:
            self.cleanup(index_value['value']['item'])
            self.index.pop(index_key, None)
            logger.warning('inspector_executor.uploading.pop.expire', extra=dict(index_value=index_value))
        elif force or now - index_value['started_at'] > self.uploading_timeout:  # force or uploading timeout
            if force:
                logger.warning('inspector_executor.uploading.reenqueue.force', extra=dict(index_value=index_value))
            else:
                logger.warning('inspector_executor.uploading.reenqueue.uploading_timeout', extra=dict(index_value=index_value))
            self.cache.push(index_value['value'], expire=expire, tag=index_value["tag"])

    def _run(self, *args, **kwargs):
        # enqueue uploading items which are timeout
        for index_key, index_value in self.index.items():
            self._decide_reenqueue(index_key)
        # dequeue to upload items
        while True:
            (key, value), expire_time, tag = self.cache.peek(expire_time=True, tag=True)
            if key is None:
                break
            index_key = tag
            index_value = self.index.get(index_key)
            if not index_value:
                self.cache.pop(key)
                continue
            index_value.update(dict(expire_time=expire_time, started_at=time.time()))
            self.index[index_key] = index_value
            success = self.handle(index_key, value['item'])
            logger.info('inspector_executor.uploading.handle', extra=dict(index_value=index_value, success=success))
            if success:
                self.cache.pop(key)
            else:
                self.index.pop(index_key, None)
                break

    def on_handled(self, index_key, success):
        logger.info('inspector_executor.uploading.on_handled',
                    extra=dict(success=success, index_key=index_key, index_value=self.index.get(index_key)))
        if success:
            self.index.pop(index_key, None)
        else:
            self._decide_reenqueue(index_key, True)

    def size(self):
        self.cache.expire()
        return len(self.index)

    def is_duplicate(self, tag):
        return self.index.get(tag)

    def pre_enqueue(self, item, tag):
        result = NewResult()
        if not tag:
            result.dm = 'enqueue_failed'
            result.status_code = 400
            result.em = 'enqueue tag is None'
        return result

    def on_enqueued(self, item, tag, value):
        index_value = dict(value=value, tag=tag)
        self.index[tag] = index_value
