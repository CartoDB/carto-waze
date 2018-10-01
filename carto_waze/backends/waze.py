import requests
import csv
import re
from shapely import geos

from .base import Backend, ALERT_FIELDS, JAM_FIELDS

SRID = 4326
geos.WKBWriter.defaults['include_srid'] = True


class Waze(Backend):
    def __init__(self, *args, url):
        data = requests.get(url=url)

        if data.status_code != 200:
            raise data.raise_for_status()
        else:
            self.data_json = data.json()

        super().__init__(*args)

    def convert(self, name):
        '''
        thanks https://stackoverflow.com/a/1176023/3647833
        '''
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def get_data(self):
        raise NotImplementedError

    def get_the_geom(self):
        raise NotImplementedError

    def get_field(self, field_name):
        if field_name in self.waze_field_names:
            return field_name
        elif self.convert(field_name) in self.waze_field_names:
            return self.convert(field_name)
        elif field_name in self.CUSTOM_FIELD:
            return self.CUSTOM_FIELD[field_name]

    def get_values(self, descriptor):
        writer = csv.writer(descriptor)
        writer.writerow(self.carto_field_names)

        for data in self.get_data():
            the_geom = self.get_the_geom(data)

            data_renamed = {}
            for field in data.keys():
                data_renamed[self.get_field(field)] = data[field]

            row = ()
            for field in self.carto_field_names:
                if field in data_renamed:
                    row = row + (data_renamed[field],)
                else:
                    row = row + (None,)
            print(row)

            writer.writerow(self.build_row_with_geom(row, the_geom.wkb_hex))


class Alert(Waze):
    common_fields = ALERT_FIELDS
    CUSTOM_FIELD = {
        'nThumbsUp': 'thumbs_up'
    }

    def get_data(self):
        if 'alerts' in self.data_json:
            return self.data_json['alerts']
        else:
            raise KeyError('Alerts not found in Waze JSON')

    def get_the_geom(self, data):
        return self.get_point(data['location'])


class Jam(Waze):
    common_fields = JAM_FIELDS
    CUSTOM_FIELD = {
        'speedKMH': 'speed_kmh'
    }

    def get_data(self):
        if 'jams' in self.data_json:
            return self.data_json['jams']
        else:
            raise KeyError('Jams not found in Waze JSON')

    def get_the_geom(self, data):
        return self.get_line(data['line'])
