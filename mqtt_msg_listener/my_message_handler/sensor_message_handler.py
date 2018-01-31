import logging
import time
from mqtt_msg_listener.my_object import SensorData, MqttMessage
from mqtt_msg_listener.my_dao import DAOInterface
from mqtt_msg_listener.helper_functions import encode_json, decode_json, get_timestamp


class SensorMessageHandler(object):
    def __init__(self):
        self.logger = logging.getLogger()
        pass

    def process(self, message_connector, topic_list, message):
        self.logger.debug("Processing message %s - %s" % (topic_list, message))

        assert isinstance(message_connector, DAOInterface)

        if len(topic_list) != 4:
            self.logger.warning("Topic does not give enough info %s" % topic_list)
            return None

        if topic_list[3] == 'lm35':
            _payload = decode_json(message.payload)
            _sensorData01 = SensorData(hub_id=topic_list[1],
                                       hub_ts=_payload['ts'],
                                       knot_id=topic_list[2],
                                       sen_id=topic_list[3],
                                       sen_typ='temperature',
                                       sen_val=float(_payload['temperature']),
                                       ts=message.ts)
            return message_connector.insert(_sensorData01)
        elif topic_list[3].lower() == 'sht':
            _payload = decode_json(message.payload)
            _sensorData01 = SensorData(hub_id=topic_list[1],
                                       hub_ts=float(_payload['ts']),
                                       knot_id=topic_list[2],
                                       sen_id=topic_list[3],
                                       sen_typ='temperature',
                                       sen_val=float(_payload['temperature']),
                                       ts=message.ts)
            _sensorData02 = SensorData(hub_id=topic_list[1],
                                       hub_ts=float(_payload['ts']),
                                       knot_id=topic_list[2],
                                       sen_id=topic_list[3],
                                       sen_typ='humidity',
                                       sen_val=float(_payload['humidity']),
                                       ts=message.ts)
            return message_connector.insert([_sensorData01, _sensorData02])
        elif topic_list[3].lower() == 'pir':
            _payload = decode_json(message.payload)
            _sensorData01 = SensorData(hub_id=topic_list[1],
                                       hub_ts=float(_payload['ts']),
                                       knot_id=topic_list[2],
                                       sen_id=topic_list[3],
                                       sen_typ='pir',
                                       sen_val=float(_payload['pir']),
                                       ts=message.ts)
            return message_connector.insert(_sensorData01)
        elif topic_list[3].lower() == 'contact':
            _payload = decode_json(message.payload)
            _sensorData01 = SensorData(hub_id=topic_list[1],
                                       hub_ts=float(_payload['ts']),
                                       knot_id=topic_list[2],
                                       sen_id=topic_list[3],
                                       sen_typ='contact',
                                       sen_val=float(_payload['contact']),
                                       ts=message.ts)
            return message_connector.insert(_sensorData01)
        else:
            self.logger.warning("Can not find any action for sensor message %s" % message)
            return None

    @staticmethod
    def _parse_payload_temp(payload):
        return payload.split(',')


if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(module)s - %(threadName)s - %(levelname)s - %(message)s')
    logging.Formatter.converter = time.gmtime

    topicList = ['sensor', 'testhub', 'testknot', 'lm35']
    lm35Payload = encode_json({'ts': 123456, 'temperature': 99})
    lm35Msg = MqttMessage(ts=123456, topic='/sensor/testhub/testknot/lm35', payload=lm35Payload)
    SensorMessageHandler().process(DAOInterface(), topicList, lm35Msg)

    topicList = ['sensor', 'testhub', 'testknot', 'sht']
    shtPayload = encode_json({'ts': 123456, 'temperature': 99, 'humidity': 99})
    shtMsg = MqttMessage(ts=123456, topic='/sensor/testhub/testknot/sht', payload=shtPayload)
    SensorMessageHandler().process(DAOInterface(), topicList, shtMsg)



