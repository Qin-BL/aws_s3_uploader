import logging
from modules.utils.result import Result
from modules.utils.ffmpeg import get_ffmpeg_hwaccels
from utils.sys_cmd import sys_command_executor
from robox import config
from robot.cache import MEDIA_CONVERT_CATEGRORY, MEDIA_CONVERT_CHECK_INTERVAL
from robot.cache_executor import CacheExecutor

logger = logging.getLogger(__name__)


class MediaConvertExecutor(CacheExecutor):

    def __init__(self, expiration, on_success):
        self.on_success = on_success
        self.timout = config.DEFAULT_CONVERT_TIMEOUT
        super().__init__(MEDIA_CONVERT_CATEGRORY, expiration, MEDIA_CONVERT_CHECK_INTERVAL)

    def handle(self, item):
        result = Result()
        try:
            file_config = item["files_config"][0]
            result.status_code, result.dm = self.convert(file_config["input"], file_config["output"],
                                                         file_config["scale"], file_config["duration"])
            logger.info("MediaConvertExecutor.handle", extra={"status_code": result.status_code, "item": item,
                                                              "err": result.dm})
            if result:
                self.on_success(item)
            return result
        except:
            result.dm = "MediaConvertExecutor.handle.failed"
            logger.error(
                result.dm,
                extra={
                    "item": item
                },
                exc_info=True,
                stack_info=True
            )
            return result

    def convert(self, input, output, scale, duration):
        hwaccels = get_ffmpeg_hwaccels()
        hwaccel = config.DEFAULT_CONVERT_METHOD
        method = '-hwaccel %s' % hwaccel
        if hwaccel not in hwaccels:
            method = ''
            logger.warning("MediaConvertExecutor.convert.warn", extra={
                "err": "not support default convert method",
                "hwaccel": hwaccel,
                "hwaccels": hwaccels
            })
        cmd = "ffmpeg -y {method} -i {input} -threads {threads} -vf scale={scale} -preset slow -crf 18 {output}".format(
            method=method, input=input, threads=config.DEFAULT_CONVERT_THREADS, scale=scale, output=output)
        timeout = self.cal_timout(duration)
        status_code, _, _ = sys_command_executor(cmd, timeout, capture_output=False)
        if status_code == 0:
            return status_code, 'ok'
        logger.error("MediaConvertExecutor.convert.error", extra={
            "cmd": cmd,
            "timeout": timeout,
        })
        return status_code, 'error'

    def cal_timout(self, duration):
        return duration * config.VR_CONVERT_PROPROTION
