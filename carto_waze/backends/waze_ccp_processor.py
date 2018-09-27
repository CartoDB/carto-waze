import logging
import csv
import psycopg2
from shapely import geos
from shapely.geometry import Point, LineString

from .base import Backend, with_datasource, with_filter

SRID = 4326
ALERT_FIELDS = ("uuid", "pub_millis", "pub_utc_date", "road_type", "location", "street", "city", "country", "magvar", "reliability", "report_description", "report_rating", "confidence", "type", "subtype", "report_by_municipality_user", "thumbs_up", "jam_uuid", "datafile_id", "type_id")
JAM_FIELDS = ("uuid", "pub_millis", "pub_utc_date", "start_node", "end_node", "road_type", "street", "city", "country", "delay", "speed", "speed_kmh", "length", "turn_type", "level", "blocking_alert_id", "line", "type", "turn_line", "datafile_id")


geos.WKBWriter.defaults['include_srid'] = True


class WazeCCPProcessor(Backend):
    def __init__(self, dbname, username, password, host=None, port=None, schema="waze"):
        self.dbname = dbname
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.schema = schema
        self.conn = None
        super().__init__()

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

    @with_filter
    @with_datasource
    def get_alerts(self, datasource, filter, descriptor):
        where_clause = " and ".join(filter)

        datasource.execute("select {alert_fields} from alerts where {where_clause} limit 3".format(alert_fields=",".join(ALERT_FIELDS), where_clause=where_clause))

        alert_writer = csv.writer(descriptor)
        alert_writer.writerow(ALERT_FIELDS + ("the_geom",))
        location_field_idx = ALERT_FIELDS.index("location")

        for alert in datasource.fetchall():
            the_geom = Point(alert[location_field_idx]["x"], alert[location_field_idx]["y"])
            geos.lgeos.GEOSSetSRID(the_geom._geom, SRID)
            alert_writer.writerow(alert + (the_geom.wkb_hex,))

    @with_filter
    @with_datasource
    def get_jams(self, datasource, filter, descriptor):
        where_clause = " and ".join(filter)

        datasource.execute("select {jam_fields} from jams where {where_clause} limit 3".format(jam_fields=",".join(JAM_FIELDS), where_clause=where_clause))

        jam_writer = csv.writer(descriptor)
        jam_writer.writerow(JAM_FIELDS + ("the_geom",))
        line_field_idx = JAM_FIELDS.index("line")

        for jam in datasource.fetchall():
            the_geom = LineString([(point["x"], point["y"]) for point in jam[line_field_idx]])
            geos.lgeos.GEOSSetSRID(the_geom._geom, SRID)
            jam_writer.writerow(jam + (the_geom.wkb_hex,))
