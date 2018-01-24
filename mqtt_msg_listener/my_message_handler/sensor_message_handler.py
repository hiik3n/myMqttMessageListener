import logging
import time
from mqtt_msg_listener.my_object import SensorData
from mqtt_msg_listener.my_dao import DAOInterface


class SensorMessageHandler(object):
    def __init__(self):
        self.logger = logging.getLogger()
        pass

    def process(self, message_connector, topic_list, payload):
        self.logger.debug("Process message %s - %s" % (topic_list, payload))

        assert isinstance(message_connector, DAOInterface)

        if len(topic_list) != 4:
            self.logger.warning("Topic does not have enough info %s" % topic_list)
            return None

        _payload = self._parse_payload_temp(payload)

        if len(_payload) != 2:
            self.logger.warning("Payload does not have enough info %s" % payload)
            return None

        _sensorData01 = SensorData(hub_id=topic_list[1],
                                   hub_rssi=-100,
                                   hub_ts=round(time.time()),
                                   knot_id=topic_list[2],
                                   knot_rssi=-100,
                                   knot_ts=round(time.time()),
                                   sen_id='sen01',
                                   sen_val=float(_payload[0]),
                                   sen_ts=round(time.time()))

        _sensorData02 = SensorData(hub_id=topic_list[1],
                                   hub_rssi=-100,
                                   hub_ts=round(time.time()),
                                   knot_id=topic_list[2],
                                   knot_rssi=-100,
                                   knot_ts=round(time.time()),
                                   sen_id='sen02',
                                   sen_val=float(_payload[1]),
                                   sen_ts=round(time.time()))

        return message_connector.insert([_sensorData01, _sensorData02])

    @staticmethod
    def _parse_payload_temp(payload):
        return payload.split(',')


if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(module)s - %(threadName)s - %(levelname)s - %(message)s')
    logging.Formatter.converter = time.gmtime

    addressList = ['sensor', 'hub01', 'knot01', 'sensor01']
    payloadStr = '123,456'

    sensorMsgHandler = SensorMessageHandler().process(DAOInterface(), addressList, payloadStr)


