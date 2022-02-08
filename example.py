"""
DBConnector example script.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2021-11-04

"""

import os
import logging

from dbconnector import DBConnector

if __name__ == "__main__":
    # Set our preferred logging format
    fmt = "%(asctime)s %(module)s %(levelname)s %(message)s"
    logging.basicConfig(format=fmt, level=os.environ.get("LOGLEVEL", "DEBUG"))

    # Connect by passing an options file (recommended)
    options_file = "example_db_options.secret"
    with DBConnector(connector_args={"option_files": options_file}, cnx_retries=3) as dbc:
        test = dbc.query("select testcol1, testcol2 from test limit 10;")
        print(test)
        test = dbc.query("select testcol1, testcol2 from test limit 10;", df=True)
        print(test)
        test = dbc.query("select * from test limit 10;")
        print(test)
        test = dbc.iud_query("insert into test (testcol2) VALUES (%s);", data=[["hello",],])
        print(test)
        test = dbc.proc(proc="test_procedure", proc_args=[12345])
        print(test)
        test = dbc.query("select notacol from test limit 10;")
        print(test)

    # Connect by passing connection args as a dict
    conn_args = {
        "host": "mysql.example.com",
        "password": "n1c3-p@ssw0rd",
        "user": "admin",
        "database": "test"
    }
    with DBConnector(connector_args=conn_args, cnx_retries=3) as dbc:
        test = dbc.query("select testcol1, testcol2 from test limit 10;")
        print(test)
