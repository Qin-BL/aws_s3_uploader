import threading, logging, os, uuid, time, random
from diskcache import Cache
from robot.cache import CACHE_DIR_BASE
from robox import config
from modules.utils.result import NewResult
logger = logging.getLogger(__name__)


def count_tag(self, tag):
    ((count,),) = self._sql('SELECT COUNT(key) FROM Cache WHERE tag = ?', [tag]).fetchall()
    return count


Cache.count_tag = count_tag


class CacheExecutor:

    def __init__(self, category, expiration, check_interval, handle=None):
        self.category = category
        self.expiration = expiration
        self.check_interval = check_interval
        self.event = threading.Event()
        self.enqueue_lock = threading.Lock()
        if handle is not None:
            self.handle = handle
        """
        tag_index=True, create tag index
        example:
            self.cache.push(value, tag="your_tag")
            self.cache.push(value, tag=None)
            self.cache.push(value)
        """
        self.cache = Cache(directory=os.path.join(CACHE_DIR_BASE, self.category), tag_index=True)

    def _run(self, *args, **kwargs):
        while True:
            (key, value), expire_time, tag = self.cache.peek(expire_time=True, tag=True)
            if key is None:
                break
            now = time.time()
            if "age" not in value or (value['upload_time'] + value['age'] +
                                      random.randint(0, config.RETRY_AFTER_RANDOM_INT)) < now:
                success = self.handle(value['item'])
                logger.info('cache_executor.uploading.handle', extra=dict(value=value, success=success,
                                                                          status_code=success.status_code))
                if self.is_success(success):
                    logger.info('cache_executor.uploading.pop', extra=dict(value=value,
                                                                           success=success))
                    self.cache.pop(key, tag=True)
                    continue
                elif not self.is_retry(success):
                    value['upload_time'] = time.time()
                    if 'age' not in value:
                        value['age'] = 2
                    else:
                        value["age"] = value["age"] * 2
                        if value["age"] > config.MAX_RETRY_AFTER:
                            value["age"] = config.MAX_RETRY_AFTER
                    logger.info('cache_executor.uploading.aging', extra=dict(value=value,
                                                                             success=success,
                                                                             age=value["age"]))
            self.reenqueue(key, value, expire_time, tag)
    
    def is_success(self, success):
        if success or success.status_code == 400 or 402 <= success.status_code < 500:
            return True
        return False
    
    def is_retry(self, success):
        if success.status_code == config.RETRY_CODE:
            return True
        return False

    def run(self, *args, **kwargs):
        while True:
            self.event.wait(timeout=self.check_interval)
            try:
                self._run(*args, **kwargs)
            except:
                logger.exception('cache_executor._run.exception', extra={})
            self.event.clear()

    def trigger(self):
        self.event.set()

    def start(self, *args, **kwargs):
        thread = threading.Thread(target=self.run, args=args, kwargs=kwargs)
        thread.setDaemon(True)
        thread.start()

    def enqueue(self, item, tag=None):
        result = NewResult()
        try:
            with self.enqueue_lock:
                pre_enqueue_result = self.pre_enqueue(item, tag)
                if not pre_enqueue_result:
                    return pre_enqueue_result
                if self.is_duplicate(tag):
                    logger.warning("cache_executor.task.duplicate", extra={
                        "enqueue_item": item,
                        "enqueue_tag": tag
                    })
                    result.em = 'item duplicate'
                    return result
                value = dict(item=item, uuid=str(uuid.uuid4()))
                logger.info('cache_executor.enqueue', extra=dict(value=value))
                result.ret = self.cache.push(value, expire=self.expiration, tag=tag)
                self.on_enqueued(item, tag, value)
        except Exception as e:
            logger.exception('cache_executor.enqueue.exception', extra={})
            result.dm = 'enqueue_error'
            result.status_code = 500
            result.em = str(e)
        self.trigger()
        return result

    def reenqueue(self, key, value, expire_time, tag, refresh=False):
        if refresh:
            expiration = self.expiration
        else:
            expiration = expire_time - time.time()
        with self.cache.transact():
            self.cache.pop(key, tag=True)
            new_key = self.cache.push(value, expire=expiration, tag=tag)
        return new_key

    # return success
    def handle(self, item):
        logger.info('default handle')
        return True

    def is_duplicate(self, tag):
        return bool(tag and self.cache.count_tag(tag))

    def pre_enqueue(self, item, tag):
        return NewResult()

    def on_enqueued(self, item, tag, value):
        pass
