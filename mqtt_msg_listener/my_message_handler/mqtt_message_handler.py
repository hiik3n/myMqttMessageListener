import logging
from mqtt_msg_listener.my_message_handler.sensor_message_handler import \
    SensorMessageHandler
from mqtt_msg_listener.my_object import MqttMessage, SENSOR_MESSAGE_TYPE


class MqttMessageHandler(object):

    def __init__(self):
        self.logger = logging.getLogger()
        self.connectorList = []

    def process(self, message):
        assert isinstance(message, MqttMessage)

        self.logger.debug("Process message %s" % repr(message))

        _topic = self._parse_topic(message.topic)
        _payload = self._parse_payload(message.payload)

        _type = _topic[0]

        if _type == SENSOR_MESSAGE_TYPE:
            _msgConnector = None
            for _connector in self.connectorList:
                if _connector.get_message_category() == SENSOR_MESSAGE_TYPE:
                    _msgConnector = _connector
            if _msgConnector is None:
                self.logger.warning("Can not find Connector for message %s" % repr(message))
                return None
            return SensorMessageHandler().process(_msgConnector, _topic, _payload)
        else:
            self.logger.warning("Can not find proper handler for message %s" % repr(message))
            return None

    def add_connector(self, connector):
        self.connectorList.append(connector)

    @staticmethod
    def _parse_topic(address):
        # return a list, 1st is Type
        return address.split('/')

    @staticmethod
    def _parse_payload(payload):
        return payload


if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(module)s - %(threadName)s - %(levelname)s - %(message)s')
    logging.Formatter.converter = time.gmtime

    mqttMsg = MqttMessage(topic='abc/def', payload='123,456'.encode('utf-8'))
    msgHandler = MqttMessageHandler()
    msgHandler.process(mqttMsg)
