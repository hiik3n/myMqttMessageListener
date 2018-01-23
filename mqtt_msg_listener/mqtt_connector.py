import logging
import paho.mqtt.client as mqtt


class MqttConnector(object):
    def __init__(self, client_id=None, host='127.0.0.1', port=1883, ca_cert=None, tls_version=None,
                 crt_file=None, key_file=None, username=None, password=None, on_message_callback=None):

        self.logger = logging.getLogger()

        self.subscribeList = []
        self.host = host
        self.port = port

        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        if on_message_callback is not None:
            self.client.on_message = on_message_callback

        if username is not None:
            self.client.username_pw_set(username, password)

        if tls_version is not None:
            self.client.tls_set(ca_certs=ca_cert, tls_version=tls_version, certfile=crt_file, keyfile=key_file)

    def add_subscribe_topic(self, topic_qos_tuple):
        self.subscribeList.append(topic_qos_tuple)

    def connect(self):
        self.logger.debug("connect")
        self.client.connect(self.host, self.port)
        self._add_subscription()
        self.client.loop_start()

    def disconnect(self):
        self.logger.debug("disconnect")
        self.client.loop_stop()
        self.client.disconnect()

    def reconnect(self):
        self.logger.debug("reconnect")
        self.client.loop_stop()
        self.client.reconnect()
        self._add_subscription()
        self.client.loop_start()

    def _add_subscription(self):
        if len(self.subscribeList) > 0:
            self.client.subscribe(self.subscribeList)
        else:
            self.client.subscribe('#')

    # The callback for when the client receives a CONNACK response from the server.
    def _on_connect(self, client, userdata, flags, rc):
        self.logger.debug("Connected with result code " + str(rc))

    # The callback for when a PUBLISH message is received from the server.
    def _on_message(self, client, userdata, msg):
        self.logger.debug("On message " + msg.topic + " " + str(msg.payload))

    def _on_disconnect(self, client, userdata, rc):
        self.logger.debug("Disconnect with result code " + str(rc))


if __name__ == "__main__":

    import time
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(module)s - %(threadName)s - %(levelname)s - %(message)s')
    logging.Formatter.converter = time.gmtime

    # MQTT Broker Config
    MQTT_HOST = "ip"
    MQTT_PORT = 8883
    MQTT_USERNAME = "user"
    MQTT_PASSWORD = "pwd"
    MQTT_CACERT = "path"
    MQTT_TLSVERSION = 5  # PROTOCOL_TLSv1_2 = 5
    MQTT_CLIENTID = "name"
    MQTT_CERT = None
    MQTT_KEY = None

    def on_message_callback(client, userdata, msg):
        assert isinstance(msg, mqtt.MQTTMessage)
        print("%s - %s - %s - %s" % (msg.mid, msg.topic, msg.qos, msg.payload.decode("utf-8")))
        # time.sleep(5)


    print("hello")
    mqttConn = MqttConnector(client_id=MQTT_CLIENTID, host=MQTT_HOST, port=MQTT_PORT,
                             username=MQTT_USERNAME, password=MQTT_PASSWORD,
                             ca_cert=MQTT_CACERT, tls_version=MQTT_TLSVERSION, crt_file=MQTT_CERT, key_file=MQTT_KEY,
                             on_message_callback=on_message_callback)

    mqttConn.connect()
    time.sleep(5)
    mqttConn.reconnect()
    time.sleep(5)
    mqttConn.disconnect()
    time.sleep(5)
    mqttConn.connect()
    while 1:
        time.sleep(15)
