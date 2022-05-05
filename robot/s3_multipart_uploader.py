import os
import copy
import math
import time
import logging
import threading
from diskcache import Index
from modules.utils.result import NewResult
from robox import config
from robot.cache import S3_MULTIPART_UPLOADER_CATEGRORY, S3_MULTIPART_UPLOADER_CHECK_INTERVAL, \
    S3_MULTIPART_UPLOADER_EXPIRATION, INDEX_DIR_BASE
from robot.cache_executor import CacheExecutor
from handlers.http_handler import RoboxHTTPHandler
from constants import S3, S3_CALLBACK_FUNC, S3_UPLOAD_STATUS

logger = logging.getLogger(__name__)


class S3MultipartUploader(CacheExecutor):

    def __init__(self, categrory=S3_MULTIPART_UPLOADER_CATEGRORY, check_interval=S3_MULTIPART_UPLOADER_CHECK_INTERVAL, expiration=S3_MULTIPART_UPLOADER_EXPIRATION):
        self.index = Index(os.path.join(INDEX_DIR_BASE, categrory))
        super().__init__(categrory, expiration, check_interval)
        self.callbacks = {}
        self.parts_lock = threading.Lock()
        self.task_res_lock = threading.Lock()
        self.create_upload_lock = threading.Lock()

    def register_callbacks(self, callback_type, callbacks):
        """
        callback_type: <constants.S3_CALLBACK_TYPES>
        callbacks:
        {
        "on_success": <method on_success>
        "on_expire": <method on_expire>
        "on_error": <method on_error>
        }
        """
        self.callbacks[callback_type] = callbacks

    def get_callbacks(self, callback_type):
        return self.callbacks.get(callback_type, {})

    def handle(self, part):
        """
        part:
        {
            'file_id': <task_id::local_file>,
            'callback_type': <constants.S3_CALLBACK_TYPES>,
            'local_file': <file path>,
            'local_file_size': <file size>,
            'local_file_name': <file name>,
            'all_parts': <all_parts>,
            'part_no': <part_no>,
            'offset': <file offset>,
            "chunksize": <chunksize>,
            'task': <enqueue task>,
            ...
        }
        """
        result = NewResult()
        try:
            task = part['task']
            callback_type = part['callback_type']
            expire = task["expire_time"] - time.time()
            # TODO: handle task finish if expire
            if self.task_need_pop(task["task_id"]) or part["upload_status"] == S3_UPLOAD_STATUS.POP:
                clear_result = self.clear(task, part)
                if not clear_result:
                    logger.error("S3MultipartUploader.clear_failed", extra={
                        "task_id": task['task_id'],
                        "part": part
                    })
                    return clear_result
                return result
            if expire <= 0:
                callback_res = self.callback(callback_type, S3_CALLBACK_FUNC.ON_EXPIRE, task, part)
                if not callback_res:
                    return callback_res
            try:
                result = self.upload(part)
                logger.info("S3MultipartUploader.handle",
                            extra={"status_code": result.status_code, "item": part['task'], "err": result.dm})
            except Exception as e:
                result.dm = "upload_error"
                result.em = str(e)
                result.status_code = 500
                logger.error(
                    "S3MultipartUploader.handle.error",
                    extra={
                        "item": part['task']
                    },
                    exc_info=True,
                    stack_info=True
                )
                callback_res = self.callback(callback_type, S3_CALLBACK_FUNC.ON_ERROR, task, part)
                if not callback_res:
                    return callback_res
        except Exception as e:
            result.dm = "logic_error"
            result.em = str(e)
            result.status_code = 500
            logger.error(
                "S3MultipartUploader.logic.error",
                extra={
                    "item": part['task']
                },
                exc_info=True,
                stack_info=True
            )
        return result

    def upload(self, part):
        logger.info("S3MultipartUploader.upload", extra={
            "part": part
        })
        result = NewResult()
        part_no = part["part_no"]
        file_id = part["file_id"]
        file_type = part["file_type"]
        filename = part["local_file_name"]
        presign_res = self.presign_url(file_id, filename, part_no, file_type)
        if not presign_res:
            return presign_res
        signed_url, upload_key, upload_id, upload_bucket = presign_res.ret
        logger.info("S3MultipartUploader.presign_res", extra={
            "file_id": file_id,
            "part_no": part_no,
            "presign_res": presign_res,
        })
        part_res, parts_res = self.get_part_and_parts(file_id, filename, part_no)
        if not part_res:
            res_upload_file = self.upload_file(part["local_file"], part, signed_url)
            if not res_upload_file:
                return res_upload_file
            part_res, parts_res = res_upload_file.ret
        complete_res = self.complete(file_id, part, parts_res, upload_bucket, upload_key, upload_id)
        if not complete_res:
            logger.error("S3MultipartUploader.complete.%s"%complete_res.dm, extra={
                "file_id": file_id,
                "result.dm": complete_res.dm,
                "result.em": complete_res.em
            })
            return complete_res
        logger.info(
            "S3MultipartUploader.upload.success",
            extra={
                "file_id": file_id,
                "part_no": part_no,
                "part_res": part_res,
            }
        )
        return result

    def enqueue(self, item, tag=None):
        """
        item:
        {
            'files_config': [
                  {
                    'file_type': <constants.TOKEN_NAME>,
                    'local_file': <file path>,
                    'local_file_size': <file size>,
                    'local_file_name': <file name>,
                    ...
                  }
            ]
            'task_id': <uuid|map_id>,
            'expire_time': timestamp,
            'on_success': <function on_success>,
            'on_error': <function on_error>,
            'on_expire': <function on_expire>
        }
        Note: callback function must be thread safe
        """
        result = NewResult()
        logger.info("S3MultipartUploader.enqueue", extra={
            "item": item
        })
        if not tag:
            tag = item["task_id"]
        with self.enqueue_lock:
            if self.is_duplicate(tag):
                result.em = 'task duplicate'
                logger.warning("S3MultipartUploader.task.duplicate", extra={
                    "enqueue_item": item,
                    "enqueue_tag": tag
                })
                return result
            self.enqueue_parts(item, tag)
            if not self.is_duplicate(tag):
                result.dm = 'enqueue_failed'
                result.status_code = 500
                result.em = 'enqueue failed'
                logger.error("S3MultipartUploader.enqueue.enqueue_failed", extra={
                    "enqueue_result": result,
                    "item": item,
                    "enqueue.tag": tag
                })
        self.trigger()
        return result

    def enqueue_part(self, part, tag):
        value = dict(item=part, id=part["file_id"])
        logger.info('S3MultipartUploader.enqueue_part', extra=dict(value=value, expiration=self.expiration))
        self.cache.push(value, expire=self.expiration, tag=tag)

    def enqueue_parts(self, item, tag):
        logger.info("S3MultipartUploader.enqueue_parts", extra={
            "item": item
        })
        with self.cache.transact():
            should_callback = True
            for file_config in item['files_config']:
                file_size = file_config["local_file_size"]
                chunksize = config.MAX_SIZE_OF_MUTIPART
                part_no = 1
                all_parts = math.ceil(file_size/chunksize)
                for offset in range(0, file_size, chunksize):
                    part = copy.deepcopy(file_config)
                    part.update({
                        "file_id": S3.FILE_ID.format(task_id=item["task_id"], filename=part['local_file_name']),
                        "all_parts": all_parts,
                        "part_no": part_no,
                        "offset": offset,
                        "chunksize": chunksize,
                        'expire_time': item["expire_time"],
                        "task": item,
                        "should_callback": should_callback,
                        "upload_status": S3_UPLOAD_STATUS.PROCESSING
                    })
                    self.enqueue_part(part, tag)
                    part_no += 1
                    should_callback = False

    def get_upload_data(self, file_id, filename):
        upload_key = S3.UPLOAD_KEY.format(file_id=file_id, filename=filename)
        return self.index.get(upload_key)

    def set_upload_data(self, file_id, filename, upload_data):
        upload_key = S3.UPLOAD_KEY.format(file_id=file_id, filename=filename)
        self.index[upload_key] = upload_data

    def presign_url(self, file_id, filename, part_no, file_type):
        logger.info("S3MultipartUploader.presign_url", extra={
                "file_id": file_id,
                "upload_filename": filename,
                "part_no": part_no
            })
        with self.create_upload_lock:
            upload_data = self.get_upload_data(file_id, filename)
            if not upload_data:
                upload_data_res = RoboxHTTPHandler.multipart_upload_create(filename, file_type)
                if not upload_data_res:
                    logger.error("S3MultipartUploader.presign_url.upload_data_res.error", extra={
                            "file_id": file_id,
                            "upload_data_res": upload_data_res
                        })
                    return upload_data_res
                upload_data = upload_data_res.ret
                self.set_upload_data(file_id, filename, upload_data)
        upload_id = upload_data["UploadId"]
        upload_key = upload_data['Key']
        upload_bucket = upload_data['Bucket']
        signed_url_res = RoboxHTTPHandler.multipart_upload_sign_url(upload_key, upload_id, part_no)
        if not signed_url_res:
            logger.error("S3MultipartUploader.presign_url.signed_url_res.error", extra={
                    "file_id": file_id,
                    "upload_key": upload_key,
                    "upload_id": upload_id,
                    "part_no": part_no,
                    "signed_url_res": signed_url_res
                })
            return signed_url_res
        signed_url_res.ret = (signed_url_res.ret["url"], upload_key, upload_id, upload_bucket)
        return signed_url_res

    def get_part_and_parts(self, file_id, filename, part_no):
        parts = self.get_parts(file_id, filename)
        if not parts:
            return None, None
        return parts.get(part_no), parts

    def get_parts(self, file_id, filename):
        parts_res_key = S3.PARTS_KEY.format(file_id=file_id, filename=filename)
        return self.index.get(parts_res_key, {})

    def set_parts(self, file_id, filename, parts_res):
        parts_res_key = S3.PARTS_KEY.format(file_id=file_id, filename=filename)
        self.index[parts_res_key] = parts_res

    def append_part(self, file_id, filename, part_res):
        part_no = part_res["PartNumber"]
        with self.parts_lock:
            parts_res = self.get_parts(file_id, filename)
            part = parts_res.get(part_no)
            if not part:
                parts_res[part_no] = part_res
            self.set_parts(file_id, filename, parts_res)
        return parts_res

    def upload_file(self, upload_file, part, signed_url):
        file_id = part["file_id"]
        part_no = part["part_no"]
        result = RoboxHTTPHandler.multipart_upload_file(signed_url, upload_file, part["local_file_size"],
                                                             part["offset"], part["chunksize"])
        if not result:
            logger.error('S3MultipartUploader.upload_file.%s' % result.dm, extra={
                    "file_id": file_id,
                    "part_no": part_no,
                    "signed_url": signed_url,
                    "upload_file": upload_file
            })
            return result
        logger.info("S3MultipartUploader.upload_file.result", extra={
            "file_id": file_id,
            "part_no": part_no,
            "upload_file": upload_file,
            "signed_url": signed_url,
            "result.status_code": result.status_code,
            "result.raw": result.raw
        })
        try:
            etag = result.headers['ETag']
        except KeyError as e:
            result.dm = "no_etag"
            result.status_code = config.RETRY_CODE
            result.em = str(e)
            logger.error("S3MultipartUploader.upload_file.%s" % result.dm, extra={
                "file_id": file_id,
                "part_no": part_no,
                "upload_file": upload_file,
                "signed_url": signed_url,
                "result.ret": result.ret,
                "result.status_code": result.status_code,
                "result.headers": result.headers
            })
            return result
        part_res = {'ETag': etag, 'PartNumber': part_no}
        parts_res = self.append_part(file_id, part["local_file_name"], part_res)
        logger.info(
            "S3MultipartUploader.upload_file.parts",
            extra={
                "file_id": file_id,
                "part_no": part_no,
                "parts": parts_res
            }
        )
        result.ret = (part_res, parts_res)
        return result

    def set_task_res(self, task_id, upload_res):
        task_res_key = S3.TASK_RES_KEY.format(task_id=task_id)
        with self.task_res_lock:
            task_res = self.index.get(task_res_key, {})
            task_res[upload_res["file_id"]] = upload_res
            self.index[task_res_key] = task_res
        return task_res

    def get_task_res(self, task_id):
        task_res_key = S3.TASK_RES_KEY.format(task_id=task_id)
        return self.index.get(task_res_key, {})

    def complete(self, file_id, part, parts, upload_bucket, upload_key, upload_id):
        logger.info(
            "S3MultipartUploader.complete",
            extra={
                "file_id": file_id,
                "part": part,
                "parts": parts,
                "upload_bucket": upload_bucket,
                "upload_key": upload_key,
                "upload_id": upload_id,
            }
        )
        result = NewResult()
        if len(parts) != part["all_parts"]:
            return self.callback_waiting(result, part)
        parts_values = list(parts.values())
        task = part['task']
        task_id = task['task_id']
        task_res = self.get_task_res(task_id)
        if len(task_res) != len(task['files_config']):
            if task_res.get(file_id):
                return self.callback_waiting(result, part)
            complete_res = RoboxHTTPHandler.multipart_upload_complete(upload_key, upload_id, parts_values)
            if not (complete_res or complete_res.dm == 'no_such_upload'):
                logger.error(
                    "S3MultipartUploader.complete.error",
                    extra={
                        "file_id": file_id,
                        "res_complete": complete_res
                    }
                )
                return complete_res
            # complete task and on_success
            upload_res = copy.copy(part)
            upload_res.update({
                "Bucket": upload_bucket,
                "Key": upload_key,
            })
            task_res = self.set_task_res(task_id, upload_res)
            logger.info("S3MultipartUploader.complete.task_res", extra={
                "file_id": file_id,
                "part": part,
                "task_res": task_res,
                "files_config": task['files_config']
            })
            if len(task_res) != len(task['files_config']):
                return self.callback_waiting(result, part)
        callback_type = part['callback_type']
        callback_res = self.callback(callback_type, S3_CALLBACK_FUNC.ON_SUCCESS, task, part, task_res)
        if not callback_res:
            return callback_res
        return result

    def callback_waiting(self, result, part):
        if part["should_callback"]:
            result.dm = 'waiting'
            result.status_code = config.RETRY_CODE
        return result

    def clear_parts(self, file_id, filename):
        upload_key = S3.UPLOAD_KEY.format(file_id=file_id, filename=filename)
        parts_key = S3.PARTS_KEY.format(file_id=file_id, filename=filename)
        self.index.pop(upload_key)
        self.index.pop(parts_key)

    def clear_task(self, task_id):
        task_res_key = S3.TASK_RES_KEY.format(task_id=task_id)
        task_status_key = S3.TASK_STATUS_KEY.format(task_id=task_id)
        self.index.pop(task_res_key)
        self.index.pop(task_status_key, None)

    def pop_if_exists(self, key):
        try:
            self.index.pop(key)
        except KeyError:
            logger.info("S3MultipartUploader.pop.KeyError", extra={"key": key})

    def get_task_count(self, task_id):
        return self.cache.count_tag(task_id)

    def pop_task(self, task_id):
        key = S3.TASK_STATUS_KEY.format(task_id=task_id)
        self.index[key] = 'pop'

    def task_need_pop(self, task_id):
        key = S3.TASK_STATUS_KEY.format(task_id=task_id)
        task_status = self.index.get(key)
        if task_status == "pop":
            return True
        return False

    def clear(self, task, part):
        logger.info("S3MultipartUploader.clear", extra={"part": part, "task": task})
        result = NewResult()
        task_id = task["task_id"]
        if not part["should_callback"]:
            return result
        if part["upload_status"] != S3_UPLOAD_STATUS.POP:
            part["upload_status"] = S3_UPLOAD_STATUS.POP
            result.dm = 'pop'
            result.status_code = config.RETRY_CODE
            return result
        with self.index.transact():
            for file_config in task['files_config']:
                local_file_name = file_config['local_file_name']
                file_id = S3.FILE_ID.format(task_id=task_id, filename=local_file_name)
                self.clear_parts(file_id, local_file_name)
            self.clear_task(task_id)
        task_res_key = S3.TASK_RES_KEY.format(task_id=task_id)
        if self.index.get(task_res_key) is not None:
            result.dm = 'clear_failed'
            result.status_code = config.RETRY_CODE
        return result

    def callback(self, callback_type, callback_func, task, part, task_res=None):
        result = NewResult()
        if not part["should_callback"]:
            return result
        task_id = task["task_id"]
        callbacks = self.get_callbacks(callback_type)
        callback = callbacks.get(callback_func, None)
        if callback:
            try:
                if callback_func == S3_CALLBACK_FUNC.ON_SUCCESS:
                    callback_res = callback(task, task_res)
                else:
                    callback_res = callback(task)
                if not callback_res:
                    logger.error("S3MultipartUploader.callback.error", extra={
                        "task": task,
                        "part": part,
                        "callback_type": callback_type,
                        "callback_func": callback_func
                    })
                    return callback_res
            except Exception as e:
                result.dm = "callback_error"
                result.em = str(e)
                result.status_code = 500
                logger.error("S3MultipartUploader.callback.error", extra={
                    "task": task,
                    "part": part,
                    "callback_type": callback_type,
                    "callback_func": callback_func
                }, exc_info=True, stack_info=True)
                return result
        # clear task
        if callback_func != S3_CALLBACK_FUNC.ON_ERROR:
            clear_result = self.clear(task, part)
            if not clear_result:
                logger.error("S3MultipartUploader.clear_failed", extra={
                    "task_id": task_id,
                    "part": part
                })
                return clear_result
        return result
