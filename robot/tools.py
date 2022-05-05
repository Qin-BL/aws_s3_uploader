import logging


from . import ROBOT_APK_MAIN_ACTIVITY_NAME, ROBOT_APK_NAME

logger = logging.getLogger(__name__)


class ByteString(str):
    def __bytes__(self):
        return bytes(self, 'utf8')


class RobotAndroidController(object):
    def __init__(self, robot_ip, default_timeout_ms=None):
        if robot_ip is None:
            raise ValueError('robot IP address is required')
        self.robot_ip = robot_ip
        self.default_timeout_ms = default_timeout_ms
        self._adb = None

    def __enter__(self):
        from adb.adb_commands import AdbCommands

        if self._adb is None:
            self._adb = AdbCommands().ConnectDevice(
                serial='%s:5555' % self.robot_ip,
                default_timeout_ms=self.default_timeout_ms
            )

        return self

    def __exit__(self, *args):
        if self._adb is not None:
            self._adb.Close()
        self._adb = None

    def adb(self):
        self.__in_scope()
        return self._adb

    def __in_scope(self):
        if self._adb is None:
            raise TypeError('Called adb() outside context manager')

    def install_apk(self, apk_path, apk_name='', progress_callback=None):
        self.__in_scope()
        adb = self.adb()

        if not apk_name:
            apk_name = ROBOT_APK_NAME

        logger.info(
            'robot.apk.uninstall', extra={
                'apk-name': apk_name,
            }
        )
        adb.Uninstall(apk_name, timeout_ms=30 * 1000)

        logger.info(
            'robot.apk.install', extra={
                'apk-name': apk_name,
            }
        )
        adb.Install(
            apk_path,
            transfer_progress_callback=progress_callback,
            timeout_ms=600 * 1000
        )

    def open_app(self, app_activity_name=''):
        self.__in_scope()
        adb = self.adb()

        if not app_activity_name:
            app_activity_name = ROBOT_APK_MAIN_ACTIVITY_NAME

        logger.info(
            'robot.apk.open', extra={'app-activity-name': app_activity_name}
        )
        open_command = (
            'am start -n "%s"'
            ' -a android.intent.action.MAIN'
            ' -c android.intent.category.LAUNCHER'
        ) % (app_activity_name)
        adb.Shell(command=open_command, timeout_ms=20 * 1000)
