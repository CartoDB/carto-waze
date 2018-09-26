import psycopg2
import logging
from .base import Backend, with_datasource, with_filter


ALERT_FIELDS = ["uuid", "pub_millis", "pub_utc_date", "road_type", "street", "city", "country", "magvar", "reliability", "report_description", "report_rating", "confidence", "type", "subtype", "report_by_municipality_user", "thumbs_up", "jam_uuid", "datafile_id", "type_id"]


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
    def get_alerts(self, datasource, filter):
        where_clause = "and " + " and ".join(filter) if len(filter) > 0 else ""

        datasource.execute("select {alert_fields}, longitude, latitude from alerts, coordinates where alerts.id = coordinates.alert_id {where_clause} limit 10".format(alert_fields=",".join(ALERT_FIELDS), where_clause=where_clause))
        return datasource.fetchall()
