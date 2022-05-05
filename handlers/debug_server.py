import json
import logging
import os

import constants
import jinja2
from flask import Flask, jsonify, render_template, request
from handlers.ws_handler import RoboxHandler
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender
from modules.utils import box, configs, control, debug_server, videos
from robot.tools import RobotAndroidController
from robox import config as rconfig

app = Flask(__name__, root_path=rconfig.BASE)

logger = logging.getLogger(__name__)


class RoboxDebugServer(
    debug_server.DebugServer, ProcessMessageReceiver, ProcessMessageSender
):
    message_id = constants.MESSAGE_ID.ROBOX_DEBUG

    @classmethod
    def initialize(cls, config, cfg):
        super(RoboxDebugServer, cls).initialize(app, config)
        cls._cfg = cfg
        configs.require_configurations(config, ['BASE'])

        cls.app.jinja_loader = jinja2.FileSystemLoader(
            os.path.join(config.BASE, 'templates')
        )

        cls.main_stream_status = None

        cls.cloud_ws_status = None

        cls.action_manager_status = None

        cls.register_handlers()
        cls.start_msg_loop()

    @classmethod
    def register_handlers(cls):
        cls.register(constants.MESSAGE_ID.CLOUD_WS, cls.handle_cloud_ws_status)
        cls.register(constants.MESSAGE_ID.ROBOX_ACTION, cls.handle_action_manager_status)

    @classmethod
    def handle_action_manager_status(cls, msg, **kwargs):
        cls.action_manager_status = msg

    @classmethod
    def handle_cloud_ws_status(cls, msg, **kwargs):
        cls.cloud_ws_status = msg

    @classmethod
    def handle_main_stream_status(cls, msg, **kwargs):
        pass

    @classmethod
    def reboot_robot(cls):
        try:
            ip_address = cls._cfg.get_config(rconfig.CONFIG_ROBOT_IP_KEY)
            with RobotAndroidController(
                ip_address, default_timeout_ms=10 * 1000
            ) as device:
                # ???
                device.Reboot()
        except Exception as e:
            return jsonify(output='error', error=str(e))
        return jsonify(output='ok')

    @classmethod
    def get_robot_video_source(cls):
        return jsonify(
            code='ok',
            uri=cls._cfg.get_config(
                rconfig.CONFIG_EXTERNAL_VIDEO_SOURCE_KEY, default=None
            )
        )

    @classmethod
    def post_robot_video_source(cls):
        data = request.form
        if len(data['uri']) < 1:
            return jsonify(code='!source rtmp uri is required.')

        cls._cfg.set_config('robox.config.override.manual', True)
        cls._cfg.set_config(
            rconfig.CONFIG_EXTERNAL_VIDEO_SOURCE_KEY, data['uri'])
        control.reboot(delay=None)
        return jsonify(code='ok')

    @classmethod
    def turn_on_auto_mode(cls):
        try:
            data = request.form
            value = not bool(int(data.get('switch', '1')))
            cls._cfg.set_config(
                'robox.config.override.manual', value
            )
            with open(rconfig.MAC_ADDRESS_CONFIG_PATH) as f:
                json.load(f)
        except Exception as e:
            ret = {
                'code': 'local config is not ready, cannot switch to AutoMode',
                'error': str(e)
            }
            return jsonify(**ret)

        control.reboot(delay=None)
        return jsonify(code='ok')


@app.route('/')
def index():
    name = box.load_box_name(rconfig.BOX_JSON_PATH)
    return render_template('index.html', name=name)


@app.route('/config')
def config_page():
    return render_template('config.html')


@app.route('/connections/internet/test')
def test_internet_connection():
    pass


@app.route('/connections/network/interfaces')
def use_ifconfig():
    pass


@app.route('/connections/websocket/cloud/status', methods=['POST'])
def cloud_ws_status():
    return jsonify(status=RoboxDebugServer.cloud_ws_status)


@app.route('/connections/websocket/robot/status', methods=['POST'])
def robot_ws_status():
    return jsonify(
        status={
            'connected': RoboxHandler.last_robot_ws_connected,
            'url': RoboxHandler.robot_ws_url
        }
    )


@app.route('/streams/tools/ffprobe', methods=['POST'])
def ffprobe_test_stream():
    data = request.form
    if len(data['uri']) < 1:
        return jsonify(output='!uri is required.')
    output_raw = videos.get_ffprobe_info(data['uri'], format='json')
    if output_raw is None:
        return jsonify(output=None)
    logger.debug(
        'ffprobe.output', extra={
            'command-output': output_raw,
        }
    )
    return jsonify(output=json.loads(output_raw))


@app.route('/robox/reboot', methods=['POST'])
def reboot_robox():
    control.reboot(delay=None)
    return jsonify(output='ok')


@app.route('/robot/reboot', methods=['POST'])
def reboot_robot():
    return RoboxDebugServer.reboot_robot()


@app.route('/robot/video/source', methods=['GET'])
def get_robot_video_source():
    return RoboxDebugServer.get_robot_video_source()


@app.route('/robot/video/source', methods=['POST'])
def post_robot_video_source():
    return RoboxDebugServer.post_robot_video_source()


@app.route('/robot/actions/status', methods=['POST'])
def get_actions_status():
    return jsonify(data=RoboxDebugServer.action_manager_status)


@app.route('/robox/auto_mode', methods=['POST'])
def turn_on_auto_mode():
    return RoboxDebugServer.turn_on_auto_mode()
