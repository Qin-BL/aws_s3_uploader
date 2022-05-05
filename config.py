import os
import sys
import yaml
import requests
import logging

import robox.config_cn
import robox.config_cn_dev
import robox.config_us_dev
import robox.config_us_prod
import robox.config_us_test
import robox.config_wanda
import robox.config_cn_test
from modules.utils import ffmpeg

logger = logging.getLogger(__name__)

NAVIGATION_IP = '192.168.0.1'
INTERACTION_IP = '192.168.0.2'
BOX_IP = "192.168.0.3"
ROBOT_POWER_SERVICE_URL = 'http://127.0.0.1:9033'


DIR = os.path.dirname(os.path.realpath(__file__))
BASE = os.path.realpath(os.path.join(DIR, '..'))

# API
API_LEVEL = 1
ROBOT_LEVEL = 0
VERSION = open(os.path.join(BASE, 'VERSION'), 'r').read().strip(' \t\r\n')
ENV = open(os.path.join(BASE, 'ENV'), 'r').read().strip(' \t\r\n')

if ENV == 'dev':
    _ENV_CONFIG = robox.config_us_dev.ENV_CONFIG
elif ENV == 'test':
    _ENV_CONFIG = robox.config_us_test.ENV_CONFIG
elif ENV == 'prod':
    _ENV_CONFIG = robox.config_us_prod.ENV_CONFIG
elif ENV == 'cn':
    _ENV_CONFIG = robox.config_cn.ENV_CONFIG
elif ENV == 'cn-dev':
    _ENV_CONFIG = robox.config_cn_dev.ENV_CONFIG
elif ENV == 'wanda':
    _ENV_CONFIG = robox.config_wanda.ENV_CONFIG
elif ENV == 'cn-test':
    _ENV_CONFIG = robox.config_cn_test.ENV_CONFIG
else:
    assert False, 'Unsupported ENV!'

config_yaml = None
# load yaml config
robox_config_path = os.path.join(BASE, 'robox_config.yaml')
if os.path.exists(robox_config_path):
    with open(robox_config_path, 'r') as f:
        config_yaml = yaml.load(f.read())
if config_yaml is not None:
    if config_yaml['ENV'] is not None:
        ENV = config_yaml['ENV']
    if config_yaml['_ENV_CONFIG'] is not None:
        _ENV_CONFIG = config_yaml['_ENV_CONFIG']
    NAVIGATION_IP = config_yaml.get("NAVIGATION_IP", NAVIGATION_IP)
    INTERACTION_IP = config_yaml.get("INTERACTION_IP", INTERACTION_IP)
    BOX_IP = config_yaml.get("BOX_IP", BOX_IP)
    ROBOT_POWER_SERVICE_URL = config_yaml.get("ROBOT_POWER_SERVICE_URL", ROBOT_POWER_SERVICE_URL)
# General
DEBUG = True
if getattr(sys, 'frozen', False):
    BASE = os.path.realpath('.')
    DEBUG = False

STATS_ID_SUFFIX = _ENV_CONFIG['stats_id_suffix']

DETECTOR_MODEL_PATH = '/etc/zenod/detector'

FILE_SERVER_PORT = 19009
FILE_UPLOAD_DIR = '/tmp'
AUTO_START = True
MAC_ADDRESS_CONFIG_PATH = '/etc/zenod/robox/robot.json'

SNAPSHOT_INTERVAL = 60 * 5  # 5 min
SNAPSHOT_UPDATE_INTERVAL = 6 * 60 * 60  # 6 hours

# VR
MAX_SIZE_OF_MUTIPART = 16 * 1024 * 1024  # 16M;The part size must be a megabyte (1024 KB) multiplied by a power of 2—for example, 1048576 (1 MB), 2097152 (2 MB), 4194304 (4 MB), 8388608 (8 MB), and so on. The minimum allowable part size is 1 MB, and the maximum is 4 GB.
VR_UUID_EXPIRE = 24 * 60 * 60  # 1 DAY
DEFAULT_CONVERT_TIMEOUT = 600  # 10min
DEFAULT_CONVERT_SCALE = '-1:720'  # 720P
VR_CONVERT_PROPROTION = 5
DEFAULT_CONVERT_METHOD = 'cuvid'
DEFAULT_CONVERT_THREADS = 2

# upload map
DEFAULT_UPLOAD_EXPIRE_TIME = 24 * 60 * 60  # 1 DAY

DEFAULT_NETWORK_SPEED = 100 * 1024  # 100LK

# Hard code network interface names and the prefered order
# This configuration is specific to current:
#     1) device model
#     2) operating system
#     3) network topology
# This configuration is subject to change when any of 1) 2) 3) is changed
POSSIBLE_INTERFACE_NAME = ['wlp2s0', 'wlp1s0', 'enp1s0', 'enp2s0', 'eno1:0', 'eno1']

# Debug server
DEBUG_SERVER_PORT = 8000
DEBUG_SERVER_ENABLED = DEBUG

# Auth
BOX_JSON_PATH = '/etc/zenod/robox/box.json'
TYPE = 'dog_sr_v1'
ENABLE_SHA1 = False

# Web
WEB_DOMAIN = _ENV_CONFIG['web_domain']

WEB_PORT = 443
WEB_PROTO = 'https'
WEB_URL = '%s://%s:%s' % (WEB_PROTO, WEB_DOMAIN, WEB_PORT)
WEB_TIMEOUT = 30

# Gateway API
GW_DOMAIN = _ENV_CONFIG['gw_domain']

GW_URL = 'https://%s' % GW_DOMAIN

# Websocket
DEFAULT_ROBOT_WS_PORT = 8081
DEFAULT_ROBOT_WS_URL = "ws://%s:%d" % (NAVIGATION_IP, DEFAULT_ROBOT_WS_PORT)
WS_DOMAIN = _ENV_CONFIG['ws_domain']
WS_SERVER_PORT = 9000

SOCKET_PORT = 443
SOCKET_PROTO = 'wss'

WEB_SOCKET_URL = '%s://%s:%s' % (SOCKET_PROTO, WS_DOMAIN, SOCKET_PORT)
SOCKET_PATH = '/channel/robot'

# Wisp
WISP_ENDPOINT = _ENV_CONFIG['wisp_endpoint']

# Detection
DETECTION_FRAME_BATCH_SIZE = 80
DETECTION_FRAME_SAMPLING_RATE = 20
DEFAULT_DETECTION_TYPES = [
    'person',
]  # None for all types of objects
DEFAULT_DETECTION_THRESHOLD = 0.5  # Default threshold for object detection
# after some succeses

# Recording
FFMPEG_VERSION = ffmpeg.get_version()
FFMPEG_HWACCL = ffmpeg.ffmpeg_with_accl(FFMPEG_VERSION)
RECORDING_DIR = os.path.realpath(os.path.join(BASE, 'recordings'))
CACHE_DIR = os.path.realpath(os.path.join(BASE, 'cache'))
CLIP_DIR = os.path.realpath(os.path.join(BASE, 'clips'))

# util cache
UTIL_CACHE_CATEGORY = os.path.join(CACHE_DIR, "util/cache")
UTIL_INDEX_CATEGORY = os.path.join(CACHE_DIR, "util/index")

# robot jwt key
ROBOT_JWT_CACHE_KEY = 'robot_jwt_token'
JWT_EXPIRE_ADVANCE = 60 * 60  # 1h

RECORDING_SEGMENT_TIME_SECONDS = 60 * 1  # 5 min
RECORDING_STORAGE_QUOTA_BYTES = 1024**5 * 3  # 3 GB
RECORDING_STORAGE_QUOTA_SECONDS = 60**2 * 24 * 1  # 1 day
RECORDING_STORAGE_CLEANUP_INTERVAL_SECONDS = 60 * 30  # 30 min

FAKE_SCAN = False

# Sensors
GPS_CHECK_INTERVAL = 10
GPS_PUSH_TIMEOUT = 0.1
GPS_DEVICE_NAME = '/dev/ttyS0'

ENABLE_NTRIP = False
NTRIP_BASE_MOUNT_IP = '132.239.152.175'
NTRIP_BASE_MOUNT_PORT = '2103'
NTRIP_MOUNT_POINT = 'P177_RTCM3'
NTRIP_USERNAME = 'CRTN_Turing'
NTRIP_PASSWORD = 'TuringSurvey'

RANGER_CHECK_INTERVAL = 20
RANGER_PUSH_TIMEOUT = 0.1
RANGER_NUM_SENSOR = 8

ENABLE_LIDAR = True
SAVE_LIDAR_DATA = True
LIDAR_DATA_DIR = os.path.join(BASE, 'lidar_data')
LIDAR_CHECK_INTERVAL = 20
LIDAR_PUSH_TIMEOUT = 0.2

MAIN_CAMERA_URI = 'rtmp://127.0.0.1:5581/live/main_camera'
MAIN_CAMERA_URI_RTSP = 'rtsp://127.0.0.1:5580/live/main_camera'
MAIN_CAMERA_SDP = os.path.join(BASE, 'static/main-camera.sdp')
EXTERNAL_VIDEO_SOURCE = (
    'rtsp://10.1.10.219:5558?videoapi=mr&h264=1000-30-1280-960'
)

CAMERA_DETECTOR_STATUS_UPDATE_TIMEOUT = 15

# S3
# None to match CPU count of a host
S3_PROCESS_COUNT = None
S3_UPLOAD_TIMEOUT = 600
UPLOAD_MAX_RETRY = 3
S3_IGNORE_NOT_EXIST = True

# RoboxStorageHandler
UPLOAD_FILE_CHECK_INTERVAL = 5  # 5 seconds

# retry_after
MAX_RETRY_AFTER = 60 * 10  # 10 min
RETRY_CODE = 1000  # network error
AGING_CODE = 1001  # request or jsondecode error
RETRY_AFTER_RANDOM_INT = 10  # 10 seconds

CONFIG_ROBOT_IP_KEY = 'robot.ip'
CONFIG_AUTO_START_KEY = 'robox.sensors.auto'
CONFIG_EXTERNAL_VIDEO_SOURCE_KEY = 'robot.video.source'
CONFIG_MAIN_CAMERA_URI_KEY = 'robox.camera.main.uri'
CONFIG_MAIN_CAMERA_URI_RTSP_KEY = 'robox.camera.main.uri.detect'
CONFIG_MAIN_CAMERA_SDP = 'robox.camera.main.sdp'
CONFIG_USE_EDGE_TPU_KEY = 'robox.detector.edgetpu'
CONFIG_POSSIBLE_INTERFACE_NAME_KEY = 'robox.possible.interface.name'
CONFIG_ROBOT_DETECTION_AUTO_SPEAK_KEY = 'robot.detection.auto.speak'
# TELEGRAF_SERVER_HOST_KEY = 'telegraf.server.host'
TELEGRAF_SERVER_PORT_KEY = 'telegraf.server.port'
ROBOX_SERVER_HOST_KEY = 'robox.server.host'
ZEROCONF_SERVER_HOSTS_KEY = 'zeroconf.server.hosts'
ZEROCONF_REQUEST_TIMEOUT = 10000  # 10s

DEFAULT_SERVER_HOST = BOX_IP

# file manager
FILE_MANAGER_INTERVAL = 10
FILE_MANAGER_EXPIRE = 60*60*24*3  # 3 days
FILE_MANAGER_PATH = os.path.realpath(os.path.join(BASE, 'file_manager'))


# robot power service
DEVICES_LIST_URL = '%s/device/devices' % ROBOT_POWER_SERVICE_URL
DEVICE_STATUS_URL = DEVICES_LIST_URL + '/'
DEVICES_SET_POWER_URL = '%s/device/devices/set_power' % ROBOT_POWER_SERVICE_URL

BOX_SERVICES = ['zenod', 'nimbo.service', 'robot_power_service', 'robox', 'slam_server']
APP_SERVICES = {
    "nimbo": {
        "ip": NAVIGATION_IP,
        "app": 'com.turingvideo.robot',
        "main_activity": 'com.turingvideo.robot/com.turingvideo.robot.app.MainActivity'
    },
    "media_service": {
        "ip": INTERACTION_IP,
        "app": 'com.turingvideo.mediaservice',
        "main_activity": 'com.turingvideo.mediaservice/.app.MediaServiceActivity'
    }
}
NO_RESULT_DEVICES = ['Robot', 'DevNUC', 'DevNav']

DEFAULT_CONFIG_DICT = {
    CONFIG_ROBOT_IP_KEY: NAVIGATION_IP,
    CONFIG_AUTO_START_KEY: AUTO_START,
    CONFIG_EXTERNAL_VIDEO_SOURCE_KEY: EXTERNAL_VIDEO_SOURCE,
    CONFIG_MAIN_CAMERA_URI_KEY: MAIN_CAMERA_URI,
    CONFIG_MAIN_CAMERA_URI_RTSP_KEY: MAIN_CAMERA_URI_RTSP,
    CONFIG_MAIN_CAMERA_SDP: MAIN_CAMERA_SDP,
    CONFIG_USE_EDGE_TPU_KEY: False,
    CONFIG_POSSIBLE_INTERFACE_NAME_KEY: POSSIBLE_INTERFACE_NAME,
    CONFIG_ROBOT_DETECTION_AUTO_SPEAK_KEY: False,
    ROBOX_SERVER_HOST_KEY: DEFAULT_SERVER_HOST,
    TELEGRAF_SERVER_PORT_KEY: 8092
}

# rewrite to file
RUNTIME_INFO_FILE = 'runtime_info.yaml'
RUNTIME_INFO = {
    "web_domain": WEB_DOMAIN,
    "web_port": WEB_PORT
}
with open(RUNTIME_INFO_FILE, 'w') as f:
    f.write(yaml.dump(RUNTIME_INFO))
