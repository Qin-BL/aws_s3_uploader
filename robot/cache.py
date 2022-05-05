from robox import config as rconfig
import os
cache_installed = os.path.exists(rconfig.CACHE_DIR)
CACHE_DIR_BASE = os.path.join(rconfig.CACHE_DIR, 'cache')
INDEX_DIR_BASE = os.path.join(rconfig.CACHE_DIR, 'index')

INSPECTIONS_CREATE_CATEGORY = 'inspections_create'
INSPECTIONS_CREATE_EXPIRATION = 24 * 60 * 60  # day
INSPECTIONS_CREATE_CHECK_INTERVAL = 60  # 60s
INSPECTIONS_FILES_CATEGORY = 'inspections_files'
INSPECTIONS_FILES_EXPIRATION = 24 * 60 * 60  # 1 day
INSPECTIONS_UPLOADING_TIMEOUT = 10 * 60  # 10 minutes
INSPECTIONS_FILES_CHECK_INTERVAL = 60  # 60s

# VR
VR_FILES_CATEGRORY = 'vr_files'
VR_CREATE_EXPIRATION = 14 * 24 * 60 * 60  # 2 weeks
VR_CHECK_INTERVAL = 60  # 60s

# media convert
MEDIA_CONVERT_CATEGRORY = 'media_convert'
MEDIA_CONVERT_CHECK_INTERVAL = 60  # seconds

# s3 multipart uploader
S3_MULTIPART_UPLOADER_CATEGRORY = 's3_multipart_uploader'
S3_MULTIPART_UPLOADER_CHECK_INTERVAL = 60  # seconds
S3_MULTIPART_UPLOADER_EXPIRATION = 7 * 24 * 60 * 60  # 1 week
