import logging
from mqtt_msg_listener.my_object import SensorData, SENSOR_MESSAGE_TYPE
from influxdb import InfluxDBClient
from mqtt_msg_listener.my_dao.dao_interface import DAOInterface


class SensorDataDAO(DAOInterface):

    def __init__(self):
        self.logger = logging.getLogger()
        self.client = None
        self.measurement = None
        self._isConnect = False
        self.messageCategory = SENSOR_MESSAGE_TYPE

    def insert(self, data):
        self.logger.debug("insert %s" % data)

        if self.client is None:
            self.logger.debug("insert - client is None")
            return None

        if isinstance(data, SensorData):
            return self.client.write_points([self._create_sensor_dict(data)])

        if isinstance(data, list):
            _dataList = []
            for _data in data:
                if isinstance(_data, SensorData):
                    _dataList.append(self._create_sensor_dict(_data))
                else:
                    self.logger.debug('Element of List is not SensorData type %s' % repr(_data))
                    continue
            if len(_dataList) > 0:
                return self.client.write_points(_dataList)

        self.logger.debug('Input data is neither SensorData nor List<SensorData> %s' % repr(data))
        return None

    def query(self, query):
        self.logger.debug("query %s" % query)

        if self.client is not None:
            return self.client.query(query)

        return None

    def connect(self, host='localhost', port=8086,
                username='root', password='root', database=None, measurement=None):
        self.logger.debug("connect")

        if database is None or measurement is None:
            logging.error('Database and Measurement is unknown')
            return False

        self.measurement = measurement

        try:
            self.client = InfluxDBClient(host, port, username, password, database)
            self._isConnect = True
        except Exception as e:
            self.client = None
            self.logger.debug("Fail to connect to Database %s" % repr(e))
            self._isConnect = False

        return self._isConnect

    def _create_sensor_dict(self, data):
        assert isinstance(data, SensorData)
        return {
            'measurement': self.measurement,
            'tags': {
                'hub_id': data.hubId,
                'knot_id': data.knotId,
                'sen_id': data.senId,
            },
            'fields': {
                'hub_ts': data.hubTs,
                'hub_rssi': data.hubRssi,
                'knot_ts': data.knotTs,
                'knot_rssi': data.knotRssi,
                'sen_ts': data.senTs,
                'sen_val': data.senVal
            }
        }

    def is_connect(self):
        return self._isConnect

    def get_message_category(self):
        return self.messageCategory


if __name__ == "__main__":
    import time
    from config import *

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(module)s - %(threadName)s - %(levelname)s - %(message)s')
    logging.Formatter.converter = time.gmtime

    logging.debug("Hello")

    sensorData01 = SensorData(hub_id='hub01', hub_rssi=-100, hub_ts=round(time.time()),
                              knot_id='knot01', knot_rssi=-100, knot_ts=round(time.time()),
                              sen_id='sen01', sen_val=100, sen_ts=round(time.time()))

    sensorData02 = SensorData(hub_id='hub01', hub_rssi=-100, hub_ts=round(time.time()),
                              knot_id='knot01', knot_rssi=-100, knot_ts=round(time.time()),
                              sen_id='sen01', sen_val=100, sen_ts=round(time.time()))
    sensorDao = SensorDataDAO()

    if sensorDao.connect(host=INFLUXDB_HOST, port=INFLUXDB_PORT,
                         username=INFLUXDB_USER, password=INFLUXDB_PWD,
                         database=INFLUXDB_DATABASE, measurement=INFLUXDB_MEASUREMENT):
        logging.debug(sensorDao.insert([sensorData01, sensorData02]))
        logging.debug(sensorDao.query("Select * from %s" % INFLUXDB_MEASUREMENT))
        logging.debug(sensorDao.query("DROP MEASUREMENT %s" % INFLUXDB_MEASUREMENT))