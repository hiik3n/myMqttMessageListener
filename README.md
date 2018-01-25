#Mqtt_Message_Listener

Mqtt client subscribe  all topics in Broker by *MqttConnector* (Convert messages from MQTT Broker into MQTT Message objects)

Filter and process received messages from MQTT Broker by *MqttMessageHandler* (Manipulate MQTT Message objects)

Insert data to Database (InfluxDB) by using *DAO* (Data Access Object) (Insert MQTT Message objects to Database)

![MqttMsgListener](https://github.com/hiik3n/myMqttMessageListener/blob/master/data/certs/MqttMsgListener.png)

### Notes:

1. Just able to run ONE one_message_callback one time
