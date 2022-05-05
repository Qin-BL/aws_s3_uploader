import os
import time
import logging
import threading
from modules.utils.result import NewResult
from robot.s3_multipart_uploader import S3MultipartUploader
from robot.cache import VR_FILES_CATEGRORY, VR_CREATE_EXPIRATION, VR_CHECK_INTERVAL
from handlers.http_handler import RoboxHTTPHandler
from constants import VR, S3_CALLBACK_TYPES, S3_CALLBACK_FUNC, S3_UPLOAD_STATUS
from robot.media_convert_executor import MediaConvertExecutor
from robox import config

logger = logging.getLogger(__name__)


class VrExecutor(S3MultipartUploader):

    def __init__(self, expiration=VR_CREATE_EXPIRATION):
        super().__init__(VR_FILES_CATEGRORY, VR_CHECK_INTERVAL, expiration)
        self.vr_convert_executor = MediaConvertExecutor(VR_CREATE_EXPIRATION, self.enqueue_mobile)
        self.append_type_lock = threading.Lock()
        self.register_callbacks(S3_CALLBACK_TYPES.VR, {
            S3_CALLBACK_FUNC.ON_SUCCESS: self.on_success,
            S3_CALLBACK_FUNC.ON_EXPIRE: self.on_expire
        })

    def append_type(self, video_uuid, video_type, data):
        with self.append_type_lock:
            type_key = VR.VR_TYPE_KEY.format(video_uuid=video_uuid)
            vr_types = self.index.get(type_key, {})
            vr_types.update({video_type: data})
            self.index[type_key] = vr_types
        return vr_types

    def on_expire(self, task):
        result = NewResult()
        try:
            file_config = task["files_config"][0]
            result = self.clear_item(task["video_uuid"], file_config["vr_file"], file_config["vr_mobile_file"], task)
        except Exception as e:
            result.dm = 'callback_error'
            result.em = str(e)
            result.status_code = 500
            logger.error("VrExecutor.on_expire.error", extra={
                "upload_task": task,
            }, exc_info=True, stack_info=True)
        return result

    def on_success(self, task, upload_res):
        logger.info(
            "VrExecutor.on_success",
            extra={
                "task": task,
                "upload_res": upload_res
            }
        )
        result = NewResult()
        vr_res = list(upload_res.values())[0]
        video_uuid = task["video_uuid"]
        if task["vr_upload_status"] == S3_UPLOAD_STATUS.POP:
            clear_res = self.clear_item(video_uuid, vr_res["vr_file"], vr_res["vr_mobile_file"], task)
            if not clear_res:
                logger.error("VrExecutor.clear_item.failed", extra={
                    "upload_task": task,
                    "video_uuid": video_uuid
                })
            return clear_res
        data = {
            "Bucket": vr_res["Bucket"],
            "Key": vr_res["Key"]
        }
        vr_types = self.append_type(video_uuid, task["type"], data)
        pc_file_data = vr_types.get(VR.VR_TYPE)
        mobile_file_data = vr_types.get(VR.VR_MOBILE_TYPE)
        if pc_file_data and mobile_file_data:
            result = RoboxHTTPHandler.create_site_floor(task["routine_execution_id"], pc_file_data, mobile_file_data,
                                                                       task["floor_id"],
                                                                       time.strftime('%Y-%m-%d',
                                                                       time.localtime(task["taken_at"])))
            if not result:
                logger.error(
                    "VrExecutor.vr_complete.res_create_site_floor.failed",
                    extra={
                        "video_uuid": video_uuid,
                        "res_create_site_floor": result
                    }
                )
                return result
            result = self.clear_item(video_uuid, vr_res["vr_file"], vr_res["vr_mobile_file"], task)
        return result

    def vr_cleanup_local_file(self, vr_file, video_uuid):
        result = NewResult()
        if os.path.exists(vr_file):
            try:
                os.remove(vr_file)
            except Exception as e:
                result.dm = "rm_file_error"
                result.status_code = 500
                result.em = str(e)
                logger.error(
                    "VrExecutor.cleanup_upload_vr.failed",
                    extra={
                        "video_uuid": video_uuid,
                        "vr_file": vr_file
                    },
                    exc_info=True,
                    stack_info=True
                )
                return result
        logger.info("VrExecutor.cleanup_upload_vr.success", extra={"local_file": vr_file, "video_uuid": video_uuid})
        return result

    def enqueue(self, item, tag=None):
        item["type"] = VR.VR_TYPE
        item["should_clear"] = False
        item["vr_upload_status"] = S3_UPLOAD_STATUS.PROCESSING
        item["files_config"][0]["callback_type"] = S3_CALLBACK_TYPES.VR
        now = time.time()
        item['expire_time'] = now + VR_CREATE_EXPIRATION
        vr_enqueue_res = super(VrExecutor, self).enqueue(item)
        if not vr_enqueue_res:
            return vr_enqueue_res
        vr_convert_enqueue_res = self.vr_convert_executor.enqueue(item, tag=item["task_id"])
        return vr_convert_enqueue_res

    def enqueue_mobile(self, item):
        item["task_id"] = item['task_id'] + '_mobile'
        item["should_clear"] = True
        item["vr_upload_status"] = S3_UPLOAD_STATUS.PROCESSING
        item["type"] = VR.VR_MOBILE_TYPE
        file_config = item["files_config"][0]
        mobile_file = file_config["vr_mobile_file"]
        item["files_config"] = [{
            "callback_type": S3_CALLBACK_TYPES.VR,
            'file_type': VR.VR_CREATE_TYPE,
            'local_file': mobile_file,
            'local_file_size': os.path.getsize(mobile_file),
            'local_file_name': os.path.basename(mobile_file),
            "vr_file": file_config["vr_file"],
            "vr_mobile_file": mobile_file
        }]
        super(VrExecutor, self).enqueue(item)

    def reenqueue(self, key, value, expire_time, tag, refresh=True):
        return super().reenqueue(key, value, expire_time, tag, refresh)

    def clear_vr_keys(self, video_uuid):
        type_key = VR.VR_TYPE_KEY.format(video_uuid=video_uuid)
        self.pop_if_exists(type_key)

    def clear_item(self, video_uuid, vr_file, vr_mobile_file, task):
        res = NewResult()
        if not task["should_clear"]:
            return res
        if task["vr_upload_status"] != S3_UPLOAD_STATUS.POP:
            task["vr_upload_status"] = S3_UPLOAD_STATUS.POP
            res.dm = 'pop'
            res.status_code = config.RETRY_CODE
            return res
        vr_clear_res = self.vr_cleanup_local_file(vr_file, video_uuid)
        if not vr_clear_res:
            return vr_clear_res
        vr_mobile_clear_res = self.vr_cleanup_local_file(vr_mobile_file, video_uuid)
        if not vr_mobile_clear_res:
            return vr_mobile_clear_res
        self.clear_vr_keys(video_uuid)
        logger.info("VrExecutor.clear_item.item.other_part.on_handled", extra={
            "video_uuid": video_uuid,
            "vr_file": vr_file
        })
        return res

    def start(self, *args, **kwargs):
        super(VrExecutor, self).start(*args, **kwargs)
        self.vr_convert_executor.start(*args, **kwargs)
