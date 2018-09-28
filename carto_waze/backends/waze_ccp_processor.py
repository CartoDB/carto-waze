import logging
import csv
import psycopg2
from shapely import geos
from shapely.geometry import Point, LineString

from .base import Backend, with_datasource, with_filter, ALERT_FIELDS, JAM_FIELDS

SRID = 4326

geos.WKBWriter.defaults['include_srid'] = True


class WazeCCPProcessor(Backend):
    def __init__(self, *args, username="waze_readonly", password="", dbname="waze_data", host="", port="", schema="waze"):
        self.username = username
        self.password = password
        self.dbname = dbname
        self.host = host
        self.port = port
        self.schema = schema
        self.conn = None
        super().__init__(*args)

    def get_datasource(self):
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(dbname=self.dbname, user=self.username, password=self.password, host=self.host, port=self.port)
            except psycopg2.Error:
                logging.error("Unable to connect to the database")
                raise

        datasource = self.conn.cursor()
        datasource.execute('set search_path to "{schema}"'.format(schema=self.schema))
        return datasource

    def get_values(self):
        raise NotImplementedError


class AlertProcessor(WazeCCPProcessor):
    fields = ALERT_FIELDS

    def __init__(self, *args, **kwargs):
        self.table_name = "alerts"
        super().__init__(*args, **kwargs)

    @with_filter
    @with_datasource
    def get_values(self, datasource, filter, descriptor):
        where_clause = " and ".join(filter)

        datasource.execute("select {alert_fields} from alerts where {where_clause} limit 3".format(alert_fields=",".join(self.field_names), where_clause=where_clause))

        alert_writer = csv.writer(descriptor)
        alert_writer.writerow(self.field_names + ["the_geom"])
        location_field_idx = self.field_names.index("location")

        for alert in datasource.fetchall():
            the_geom = Point(alert[location_field_idx]["x"], alert[location_field_idx]["y"])
            geos.lgeos.GEOSSetSRID(the_geom._geom, SRID)
            alert_writer.writerow(alert + (the_geom.wkb_hex,))


class JamProcessor(WazeCCPProcessor):
    fields = JAM_FIELDS

    def __init__(self, *args, **kwargs):
        self.table_name = "jams"
        super().__init__(*args, **kwargs)

    @with_filter
    @with_datasource
    def get_values(self, datasource, filter, descriptor):
        where_clause = " and ".join(filter)

        datasource.execute("select {jam_fields} from jams where {where_clause} limit 3".format(jam_fields=",".join(self.field_names), where_clause=where_clause))

        jam_writer = csv.writer(descriptor)
        jam_writer.writerow(self.field_names + ["the_geom"])
        line_field_idx = self.field_names.index("line")

        for jam in datasource.fetchall():
            the_geom = LineString([(point["x"], point["y"]) for point in jam[line_field_idx]])
            geos.lgeos.GEOSSetSRID(the_geom._geom, SRID)
            jam_writer.writerow(jam + (the_geom.wkb_hex,))
