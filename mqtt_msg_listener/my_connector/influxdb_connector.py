import logging
from influxdb import InfluxDBClient


class InfluxDbConnector(object):
    def __init__(self, host='localhost', port=8086, username='root', password='root', database=None):
        self.logger = logging.getLogger()
        self.client = InfluxDBClient(host, port, username, password, database)

    def get_list_database(self):
        return [db['name'] for db in self.client.get_list_database()]

    def check_if_database(self, database):
        if database in self.get_list_database():
            return True
        else:
            return False

    def write_point(self, json_data):
        return self.client.write(json_data)

    def write_points(self, json_datas):
        return self.client.write_points(json_datas)

    def select_all(self, measurement):
        return self.client.query('select * from %s;' % measurement)

    def drop_measurement(self, measurement):
        self.client.drop_measurement(measurement)


if __name__ == "__main__":
    import time
    from config import *

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(module)s - %(threadName)s - %(levelname)s - %(message)s')
    logging.Formatter.converter = time.gmtime

    logging.debug("Hello")
    dbConn = InfluxDbConnector(host=INFLUXDB_HOST, port=INFLUXDB_PORT,
                               username=INFLUXDB_USER, password=INFLUXDB_PWD,
                               database=INFLUXDB_DATABASE)
    dataList = []
    data = {}
    data['measurement'] = 'test'
    print(round(time.time() * 1000))
    data['time'] = round(time.time())
    data['tags'] = {}
    data['tags']['host'] = 'server'
    data['fields'] = {}
    data['fields']['value'] = 0.64

    dataList.append(data)

    logging.debug(dbConn.write_points(dataList))
    logging.debug(dbConn.select_all("test"))
    logging.debug(dbConn.drop_measurement("test"))
    # while 1:
    #     time.sleep(15)