SENSOR_MESSAGE_TYPE = 'sensor'


class SensorData(object):
    def __init__(self, *args, **kwargs):

        # record info
        self.id = kwargs['id'] if 'id' in kwargs.keys() else None
        self.ts = kwargs['ts'] if 'ts' in kwargs.keys() else None

        # hub info
        self.hubId = kwargs['hub_id'] if 'hub_id' in kwargs.keys() else None
        self.hubTs = kwargs['hub_ts'] if 'hub_ts' in kwargs.keys() else None
        self.hubRssi = kwargs['hub_rssi'] if 'hub_rssi' in kwargs.keys() else None

        # knot info
        self.knotId = kwargs['knot_id'] if 'knot_id' in kwargs.keys() else None
        self.knotTs = kwargs['knot_ts'] if 'knot_ts' in kwargs.keys() else None
        self.knotRssi = kwargs['knot_rssi'] if 'knot_rssi' in kwargs.keys() else None

        # sensor info
        self.senId = kwargs['sen_id'] if 'sen_id' in kwargs.keys() else None
        self.senTs = kwargs['sen_ts'] if 'sen_ts' in kwargs.keys() else None
        self.senVal = kwargs['sen_val'] if 'sen_val' in kwargs.keys() else None
