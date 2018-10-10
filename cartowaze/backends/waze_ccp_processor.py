import logging
import csv
import psycopg2

from .base import Backend, with_datasource, with_filter, ALERT_FIELDS, JAM_FIELDS


class WazeCCPProcessor(Backend):
    table_name = ""

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

    @with_filter
    @with_datasource
    def get_values(self, datasource, filter, descriptor):
        where_clause = " and ".join(filter)

        datasource.execute("select {columns} from {table_name} where {where_clause}".format(columns=",".join(self.waze_field_names), table_name=self.table_name, where_clause=where_clause))

        csv_writer = csv.writer(descriptor)
        csv_writer.writerow(self.carto_field_names)

        for row in datasource.fetchall():
            location = self.get_location(row)
            the_geom = self.get_the_geom(location)
            csv_writer.writerow(self.build_row_with_geom(row, the_geom.wkb_hex))

    def get_location(self, row):
        for i, column in enumerate(self.waze_field_names):
            if column == self.location_field:
                return row[i]

    def get_the_geom(self):
        raise NotImplementedError


class AlertProcessor(WazeCCPProcessor):
    common_fields = ALERT_FIELDS
    table_name = "alerts"

    def __init__(self, *args, **kwargs):
        self.carto_table_name = "alerts"
        self.table_name_name = "alerts"
        super().__init__(*args, **kwargs)

    def get_the_geom(self, location):
        return self.get_point(location)


class JamProcessor(WazeCCPProcessor):
    common_fields = JAM_FIELDS
    location_field = "line"
    table_name = "jams"

    def __init__(self, *args, **kwargs):
        self.table_name = "jams"
        super().__init__(*args, **kwargs)

    def get_the_geom(self, location):
        return self.get_line(location)
