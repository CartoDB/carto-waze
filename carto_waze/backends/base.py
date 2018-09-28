from carto.sql import SQLClient, CopySQLClient


ALERT_FIELDS = {
    "uuid": "text",
    "pub_millis": "bigint",
    "pub_utc_date": "timestamp without time zone",
    "road_type": "integer",
    "location": "jsonb",
    "street": "text",
    "city": "text",
    "country": "text",
    "magvar": "integer",
    "reliability": "integer",
    "report_description": "text",
    "report_rating": "integer",
    "confidence": "integer",
    "type": "text",
    "subtype": "text",
    "report_by_municipality_user": "boolean",
    "thumbs_up": "integer",
    "jam_uuid": "text",
    "datafile_id": "bigint",
    "type_id": "integer"
}

JAM_FIELDS = {
    "uuid": "text",
    "pub_millis": "bigint",
    "pub_utc_date": "timestamp without time zone",
    "start_node": "text",
    "end_node": "text",
    "road_type": "integer",
    "street": "text",
    "city": "text",
    "country": "text",
    "delay": "integer",
    "speed": "real",
    "speed_kmh": "real",
    "length": "integer",
    "turn_type": "text",
    "level": "integer",
    "blocking_alert_id": "text",
    "line": "jsonb",
    "type": "text",
    "turn_line": "jsonb",
    "datafile_id": "bigint"
}


def with_datasource(method):
    def method_wrapper(self, *args, **kwargs):
        with self.get_datasource() as datasource:
            return method(self, datasource, *args, **kwargs)

    return method_wrapper


def with_filter(method):
    def method_wrapper(self, *args, **kwargs):
        filter = []

        for filter_left, value in kwargs.items():
            try:
                field, operator = filter_left.split("__")
            except ValueError:
                field, operator = filter_left, "="
            else:
                if operator == "eq":
                    operator = "="
                elif operator == "neq":
                    operator = "!="
                elif operator == "gt":
                    operator = ">"
                elif operator == "gte":
                    operator = ">="
                elif operator == "lt":
                    operator = "<"
                elif operator == "lte":
                    operator = "<="
            filter.append(field + operator + str(value))
        return method(self, filter, *args)

    return method_wrapper


class Backend:
    fields = None

    def __init__(self, carto_auth_client):
        self.carto_auth_client = carto_auth_client
        self.datasource = None
        self.table_name = ""

    @property
    def field_names(self):
        return list(self.fields.keys())

    def get_datasource(self):
        return self.datasource

    def get_values(self):
        raise NotImplementedError

    def create_table(self, table_name=None, cartodbfy=False):
        table_name = table_name or self.table_name
        client = SQLClient(self.carto_auth_client)

        client.send("CREATE TABLE IF NOT EXISTS {table_name} (the_geom geometry(Geometry, 4326),{columns})".format(table_name=table_name, columns=",".join([name + " " + value for name, value in self.fields.items()])))
        if cartodbfy is True:
            client.send("SELECT CDB_CartodbfyTable({schema}, '{table_name}')".format(schema=self.carto_auth_client.username, table_name=table_name))

    def append_data(self, descriptor, table_name=None):
        table_name = table_name or self.table_name
        client = CopySQLClient(self.carto_auth_client)

        query = "COPY {table_name} FROM stdin WITH (FORMAT csv, HEADER true)".format(table_name=table_name)
        client.copyfrom_file_object(query, descriptor)
