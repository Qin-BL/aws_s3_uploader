import logging
import constants

from sensors.g5st import G5STSensor
from sensors.infrared_thermo.infrared_thermo import InfraredThemo
from modules.infra.message.receiver import ProcessMessageReceiver
from modules.infra.message.sender import ProcessMessageSender

logger = logging.getLogger(__name__)

INFRARED_THERMO_DEVICE_IP = "10.43.0.200"

class RoboxSensorHandler(ProcessMessageReceiver, ProcessMessageSender):
    message_id = constants.MESSAGE_ID.ROBOX_SENSOR
    infrared_thermo = None

    @classmethod
    def initialize(cls):
        cls.register_handlers()
        cls.start_msg_loop()
        cls.infrared_thermo = InfraredThemo()
        if cls.infrared_thermo.start(INFRARED_THERMO_DEVICE_IP):
            logger.info('robot.sensor.infared_thermo.started')
        else:
            logger.warning('robot.sensor.infared_thermo.not_found')

    @classmethod
    def terminate(cls):
        cls.stop_msg_loop()
        cls.infrared_thermo.stop()
        cls.infrared_thermo = None

    @classmethod
    def register_handlers(cls):
        cls.register(
            constants.SENSOR.ROBOT_REPORT,
            cls.handle_robot_sensor,
        )

    @classmethod
    def handle_robot_sensor(cls, msg, **kwargs):
        sensor_types = msg['point']['sensor_types']
        for sensor_type in sensor_types.split(','):
            if sensor_type == 'g5st':
                data = {}

                # insert sensor readings from g5st
                g5st_readings = G5STSensor.query()
                if g5st_readings:
                    data.update(g5st_readings)

                # insert temperature reading from infrared thermometer
                infrared_max_temperature = cls.infrared_thermo.get_max_temperature()
                if infrared_max_temperature:
                    data.update(infrared_max_temperature)

                # update sensor message body
                if data:
                    msg['point']['data'].update(data)
                    msg['sensor_type'] = 'g5st'
                    logger.info(
                        'robot.sensor',
                        extra={
                            'msg-payload': msg,
                            'msg-kwargs': kwargs,
                        }
                    )
                    cls.send_msg(constants.MESSAGE_ID.ROBOX_HTTP, constants.STATUS.SENSOR, msg, **kwargs)
                else:
                    logger.error(
                        'robot.sensor.failure',
                        extra={
                            'msg-payload': msg,
                            'msg-kwargs': kwargs,
                        }
                    )
