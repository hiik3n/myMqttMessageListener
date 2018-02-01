import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'mqtt_msg_listener'))
import time
import logging
from config import *
from mqtt_msg_listener.my_connector import MqttConnector, mqtt
from mqtt_msg_listener.my_message_handler import MqttMessageHandler
from mqtt_msg_listener.my_queue import MyQueue
from mqtt_msg_listener.my_object import MqttMessage
from mqtt_msg_listener.my_dao import SensorDataDAO
from mqtt_msg_listener.helper_functions import get_timestamp

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(module)s - %(threadName)s - %(levelname)s - %(message)s')
logging.Formatter.converter = time.gmtime


def on_message_callback(client, userdata, msg):
    global mqttReceiveQueue
    assert isinstance(mqttReceiveQueue, MyQueue)
    assert isinstance(msg, mqtt.MQTTMessage)
    logging.debug("GET %s - %s - %s - %s" % (msg.mid, msg.topic, msg.qos, msg.payload.decode("utf-8")))
    mqttReceiveQueue.put(MqttMessage(mid=msg.mid, topic=msg.topic,
                                     qos=msg.qos, payload=msg.payload.decode("utf-8"), ts=get_timestamp()))


logging.info("Hello")

logging.debug("Initial instance mqttReceiveQueue")
mqttReceiveQueue = MyQueue()

logging.debug("Initial instance sensorDao")
sensorDao = SensorDataDAO()

logging.debug("Initial instance mqttConn")
mqttConn = MqttConnector(client_id=MQTT_CLIENTID, host=MQTT_HOST, port=MQTT_PORT,
                         username=MQTT_USERNAME, password=MQTT_PASSWORD,
                         ca_cert=MQTT_CACERT, tls_version=MQTT_TLSVERSION, crt_file=MQTT_CERT, key_file=MQTT_KEY,
                         on_message_callback=on_message_callback)

logging.debug("Initial instance mqttMsgHandler")
mqttMsgHandler = MqttMessageHandler()
mqttMsgHandler.add_connector(sensorDao)

res = mqttConn.connect()
logging.debug("Connected to MQTT Broker (code=%s)" % res)

res = sensorDao.connect(host=INFLUXDB_HOST, port=INFLUXDB_PORT,
                        username=INFLUXDB_USER, password=INFLUXDB_PWD,
                        database=INFLUXDB_DATABASE, measurement=INFLUXDB_MEASUREMENT)
logging.debug("Connected to Database (code=%s)" % res)
time.sleep(5)

while 1:
    # logging.debug("Hi")
    #
    # logging.debug("Check the connection to Mqtt Broker (%s), Database (%s)" %
    #               (mqttConn.is_connect(), sensorDao.is_connect()))

    while not mqttReceiveQueue.is_empty():
        mqttMsgHandler.process(mqttReceiveQueue.get())

    time.sleep(0.5)

