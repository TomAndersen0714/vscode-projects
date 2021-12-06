import binascii
import datetime
import decimal
import uuid
from urllib.parse import urlunparse, urlparse
import json
import redis
import simplejson
from impala.error import DatabaseError, RPCError
from sqlalchemy.orm import Query

try:
    from impala.dbapi import connect
    from impala.error import DatabaseError, RPCError
    enabled = True
except ImportError as e:
    enabled = False


TYPE_INTEGER = 'integer'
TYPE_FLOAT = 'float'
TYPE_BOOLEAN = 'boolean'
TYPE_STRING = 'string'
TYPE_DATETIME = 'datetime'
TYPE_DATE = 'date'
SUPPORTED_COLUMN_TYPES = set([
    TYPE_INTEGER,
    TYPE_FLOAT,
    TYPE_BOOLEAN,
    TYPE_STRING,
    TYPE_DATETIME,
    TYPE_DATE
])
COLUMN_NAME = 0
COLUMN_TYPE = 1

types_map = {
    'BIGINT': TYPE_INTEGER,
    'TINYINT': TYPE_INTEGER,
    'SMALLINT': TYPE_INTEGER,
    'INT': TYPE_INTEGER,
    'DOUBLE': TYPE_FLOAT,
    'DECIMAL': TYPE_FLOAT,
    'FLOAT': TYPE_FLOAT,
    'REAL': TYPE_FLOAT,
    'BOOLEAN': TYPE_BOOLEAN,
    'TIMESTAMP': TYPE_DATETIME,
    'CHAR': TYPE_STRING,
    'STRING': TYPE_STRING,
    'VARCHAR': TYPE_STRING
}


class JSONEncoder(simplejson.JSONEncoder):
    """Adapter for `simplejson.dumps`."""

    def default(self, o):
        # Some SQLAlchemy collections are lazy.
        if isinstance(o, Query):
            result = list(o)
        elif isinstance(o, decimal.Decimal):
            result = float(o)
        elif isinstance(o, (datetime.timedelta, uuid.UUID)):
            result = str(o)
        # See "Date Time String Format" in the ECMA-262 specification.
        elif isinstance(o, datetime.datetime):
            result = o.isoformat()
            if o.microsecond:
                result = result[:23] + result[26:]
            if result.endswith('+00:00'):
                result = result[:-6] + 'Z'
        elif isinstance(o, datetime.date):
            result = o.isoformat()
        elif isinstance(o, datetime.time):
            if o.utcoffset() is not None:
                raise ValueError("JSON can't represent timezone-aware times.")
            result = o.isoformat()
            if o.microsecond:
                result = result[:12]
        elif isinstance(o, memoryview):
            result = binascii.hexlify(o).decode()
        elif isinstance(o, bytes):
            result = binascii.hexlify(o).decode()
        else:
            result = super(JSONEncoder, self).default(o)
        return result


def json_dumps(data, *args, **kwargs):
    """A custom JSON dumping function which passes all parameters to the
    simplejson.dumps function."""
    kwargs.setdefault('cls', JSONEncoder)
    kwargs.setdefault('encoding', None)
    return simplejson.dumps(data, *args, **kwargs)


def run_query(conn, query):
    try:
        cursor = conn.cursor()

        cursor.execute(query)

        column_names = []
        columns = []

        for column in cursor.description:
            column_name = column[COLUMN_NAME]
            column_names.append(column_name)

            columns.append({
                'name': column_name,
                'friendly_name': column_name,
                'type': types_map.get(column[COLUMN_TYPE], None)
            })

        rows = [dict(zip(column_names, row)) for row in cursor]

        # data = {'columns': columns, 'rows': rows}
        # json_data = json_dumps(rows)
        json_data = rows
        error = None
        cursor.close()
    except DatabaseError as e:
        json_data = None
        error = e.message
    except RPCError as e:
        json_data = None
        print(e)
        error = "Metastore Error"
    except KeyboardInterrupt:
        conn.cancel()
        error = "Query cancelled by user."
        json_data = None
    finally:
        pass

    return json_data, error


def get_tables():
    schema_dict = {}
    schemas_query = "show schemas;"
    tables_query = "show tables in %s;"
    columns_query = "show column stats %s.`%s`;"
    connection = connect(host=host, port=21050)

    schemas, err = run_query(connection, schemas_query)
    if err is not None:
        print("error")
        exit(1)

    for schema_name in [str(a['name']) for a in schemas]:
        if schema_name in ["_impala_builtins", "default", "app_crm"]:
            continue
        tables, err = run_query(connection, tables_query % schema_name)
        for table_name in [str(a['name']) for a in tables]:
            cols, err = run_query(connection, columns_query % (schema_name, table_name))
            columns = [str(a['Column']) for a in cols]
            table_name = '{}.{}'.format(schema_name, table_name)
            print(table_name)
            schema_dict[table_name] = {'name': table_name, 'columns': columns}
    connection.close()

    return list(schema_dict.values())


def add_decode_responses_to_redis_url(url):
    """Make sure that the Redis URL includes the `decode_responses` option."""
    parsed = urlparse(url)

    query = 'decode_responses=True'
    if parsed.query and 'decode_responses' not in parsed.query:
        query = "{}&{}".format(parsed.query, query)
    elif 'decode_responses' in parsed.query:
        query = parsed.query

    return urlunparse([parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment])


def write_json_file(records, file_path):
    with open(file_path, mode="w", encoding="UTF-8") as fo:
        for i in records:
            fo.write(json.dumps(i, ensure_ascii=False) + "\n")


host = "10.20.2.30"

if __name__ == '__main__':
    ret = sorted(get_tables(), key=lambda t: t['name'])
    # tmp = []
    # for i in ret:
    #     if not i["name"].startswith("dim"):
    #         continue
    #     print(i)
    #     tmp.append(i)
    write_json_file(ret, "./schemas.json")
    REDIS_URL = add_decode_responses_to_redis_url("redis://10.20.0.174:6379/0")
    redis_connection = redis.from_url(REDIS_URL)
    redis_connection.set("data_source:schema:4", json_dumps(ret))

