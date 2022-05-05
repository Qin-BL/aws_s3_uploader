import logging

import constants
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender
from turingvideo.stats import stats as sts

logger = logging.getLogger(__name__)
stats = sts.get_client(__name__)


class RoboxIncidentHandler(ProcessMessageReceiver, ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX_INCIDENT

    @classmethod
    def initialize(cls):
        cls.register_handlers()
        cls.start_msg_loop()

    @classmethod
    def register_handlers(cls):
        cls.register(
            constants.INCIDENT.ROBOT_REPORT,
            cls.handle_robot_incident,
        )

    @classmethod
    def handle_robot_incident(cls, msg, **kwargs):
        logger.info(
            'robot.incident',
            extra={
                'msg-payload': msg,
                'msg-kwargs': kwargs,
            }
        )
        arg = msg['arg']
        incident_type = arg.get('type', 'unknown')
        stats.incr('robot.incident.' + incident_type, 1)

        cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.STATUS.INCIDENT, arg, **kwargs)

        ret_data = msg
        ret_data['act'] += '!'
        ret_data['ret'] = {}
        ret_data['err'] = {'dm': 'ok', 'ec': 0, 'em': 'ok'}
        cls.send_msg(constants.MESSAGE_ID.ROBOT_WS, constants.WS_SEND, ret_data, device='robot', **kwargs)
