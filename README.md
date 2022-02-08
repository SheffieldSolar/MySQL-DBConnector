# MySQL-DBConnector

A  wrapper for the MySQL Python connector that provides additional resilience and functionality.

**Latest version: 1.3.1**

## About this repository

* A wrapper for the MySQL Python connector that provides additional resilience and functionality.
* Implements connection pooling that's more reliable.
* Developed and tested with Python 3.9, should work with 3.5+

## How do I get set up?

* Make sure you have Git installed - [Download Git](https://git-scm.com/downloads)
* Run `pip install git+https://github.com/SheffieldSolar/MySQL-DBConnector/`

## Usage
Start by creating an instance of the `DBConnector` class, ideally using a context manager as this will ensure all connections are closed if an error is encountered or the code is interrupted:

```Python
from dbconnector import DBConnector
```

The `DBConnector` class requires at least one argument: `connector_args`. This should be a dictionary of connection arguments, the full list of which can be found [here](https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html). Most of these are optional, but you'll need to specify at least:

```Python
connector_args = {
    "user": "<your-username>",
    "password": "<your-password>",
    "database": "<your-database>",
}
```

Some of the connector args have default values which override the defaults in the mysql-connector-python library, they are:

- `time_zone`: "UTC"
- `connection_timeout`: 60
- `buffered`: True
- `get_warnings`: True
- `raise_on_warnings`: False
- `use_pure`: True

You can override these yourself if you prefer.

Loading your password into your Python code is usually insecure, so I recommend using a MySQL options file to pass the connection parameters rather than hard-coding the DB credentials:
```Python
from dbconnector import DBConnector

mysql_options_file = "mysql_options.txt"

# HINT: Use the context manager to ensure DB connections are always safely closed

with DBConnector(connector_args={"option_files": mysql_options_file}) as dbc:
    # Selecting from the database (returns a list of tuples):
    # See https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-fetchall.html
    # Note that using `select * from` is not recommended since the returned columns or their order
    # might change.
    data = dbc.query("SELECT * FROM test;")

    # Select from the database and convert to a Pandas DataFrame:
    data = dbc.query("SELECT col2, col2 FROM test;", df=True)

    # Inserting one row into the database:
    dbc.iud_query("insert into test (col1, col2, col3) values ('val1', 'val2', 'val3');")

    # Inserting many rows into the database:
    data_to_insert = [[1, 'row1'], [2, 'row2'], [3, 'row3'], [4, 'row4']]
    dbc.iud_query("insert into test ('col1', 'col2') values (%s, %s);", data_to_insert)
    
    # Execute a procedure with arguments:
    args = (1, 2, 3)
    result = dbc.proc("myProcedure", args)
```

The file _'mysql_options.txt'_ should look something like this:

```
[client]
host        = <your-server-hostname>
user        = <your-username>
password    = <your-password>
database    = <your-database>
```

It is also possible to pass connection parameters directly to the DBConnector class. This is not recommended but can be useful for testing/debugging:

```Python
from dbconnector import DBConnector

# HINT: Always specify the time_zone arg to avoid issues with MySQL's timezone-naive datetimes or
# unwanted TZ conversions of timestamp fields!
connector_args = {
    "host": "<your-server-hostname>",
    "user": "<your-username>",
    "password": "<your-password>",
    "database": "<your-database>",
    "time_zone": "UTC"
}

with DBConnector(connector_args=connector_args) as dbc:
    # Do some DB stuff
```

The `dbconnector` module uses the Python `logging` module to log useful debugging messages, warnings and errors. You can set your preferred logging level using something like:

```Python
import os
import logging

fmt = "%(asctime)s %(module)s %(levelname)s %(message)s"
logging.basicConfig(format=fmt, level=os.environ.get("LOGLEVEL", "DEBUG"))
```

## Documentation

- [https://sheffieldsolar.github.io/MySQL-DBConnector/](https://sheffieldsolar.github.io/MySQL-DBConnector/)

## How do I update?

- Run `pip install --upgrade git+https://github.com/SheffieldSolar/MySQL-DBConnector/`

## Who do I talk to?

- [Jamie Taylor](https://github.com/JamieTaylor-TUOS) - [jamie.taylor@sheffield.ac.uk](mailto:jamie.taylor@sheffield.ac.uk "Email Jamie") - [SheffieldSolar](https://github.com/SheffieldSolar)

## Authors

- [Jamie Taylor](https://github.com/JamieTaylor-TUOS) - [SheffieldSolar](https://github.com/SheffieldSolar)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
