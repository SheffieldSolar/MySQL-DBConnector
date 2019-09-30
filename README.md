# MySQL-DBConnector #

A  wrapper for the MySQL Python connector that provides additional resilience and functionality.

## What is this repository for? ##

* A wrapper for the MySQL Python connector that provides additional resilience and functionality.
* Implements connection pooling that's more reliable.
* Version 1.1.4
* Works with Python 2.7+ or 3.5+

## How do I get set up? ##

* Make sure you have Git installed - [Download Git](https://git-scm.com/downloads)
* Run `pip install git+https://github.com/SheffieldSolar/MySQL-DBConnector/`

## Getting started ##
I recommend using a MySQL options file to pass the connection parameters rather than hard-coding the DB credentials:
```Python
from dbconnector import DBConnector

mysql_options_file = "mysql_options.txt"

# Use the context manager to ensure DB connections are always safely closed
# Always specify the session_tz to avoid issues with MySQL's timezone-naive datetimes or unwanted TZ conversions of timestamp fields!
with DBConnector(mysql_options_file, session_tz="UTC") as dbc:
    # Selecting from the database (returns a list of tuples):
    # See https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-fetchall.html
    data = dbc.query("SELECT * FROM test;")

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

db_config={"host": "<your-server-hostname>", "user": "<your-username>",
           "password": "<your-password>", "database": "<your-database>"}

with DBConnector(db_config=db_config, session_tz="UTC") as dbc:
    # Do some DB stuff
```

## Documentation ##

* [https://sheffieldsolar.github.io/MySQL-DBConnector/](https://sheffieldsolar.github.io/MySQL-DBConnector/)

## How do I update? ##

* Run `pip install --upgrade git+https://github.com/SheffieldSolar/MySQL-DBConnector/`

## Who do I talk to? ##

* Jamie Taylor - [jamie.taylor@sheffield.ac.uk](mailto:jamie.taylor@sheffield.ac.uk "Email Jamie") - [SheffieldSolar](https://github.com/SheffieldSolar)

## Authors ##

* **Jamie Taylor** - *Initial work* - [SheffieldSolar](https://github.com/SheffieldSolar)

## License ##

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
