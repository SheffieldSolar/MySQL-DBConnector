"""
A wrapper for the MySQL Python connector that provides additional resilience and
functionality.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2015-01-16

"""

from datetime import datetime
import time as TIME
import os
import sys
import inspect
import mysql.connector
from mysql.connector import errorcode, connection

class GenericErrorLogger:
    """Basic error logging to a file (optional)."""
    def __init__(self, filename):
        self.logfile = filename

    def write_to_log(self, msg):
        """Log the error message to the logfile along with a datestamp and the name of the script
        (in case of shared logfiles).

        Parameters
        ----------
        `msg` : string
            Message to be recorded in *self.logfile*.
        """
        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        scriptname = os.path.basename(__file__)
        fid = open(self.logfile, 'a')
        fid.write(timestamp + " " + scriptname + ": " + str(msg) + "\n")
        fid.close()
        return

class DBConnectorException(Exception):
    """An Exception specific to the DBConnector class."""
    def __init__(self, logfile, msg):
        try:
            caller_file = inspect.stack()[2][1]
        except:
            caller_file = os.path.basename(__file__)
        self.msg = "%s (in '%s')" % (msg, caller_file)
        if logfile:
            logger = GenericErrorLogger(logfile)
            logger.write_to_log(self.msg)
    def __str__(self):
        return self.msg

class DBConnectorLog:
    """Record, read and analyse DBConnector stats."""
    def __init__(self, logfile, db_config):
        self.logfile = logfile
        self.status = {0: "success", 1: "error", 2: "warning"}
        if "database" in db_config:
            self.host = "{}.{}.{}".format(db_config["host"], db_config["user"],
                                          db_config["database"])
        else:
            _, self.host = os.path.split(db_config["option_files"])

    def log_query(self, sql, start, time_taken, status=0, error=None):
        """Append query details to the log file."""
        if self.logfile is None:
            return
        status = self.status[status]
        with open(self.logfile, "ab") as log:
            log.write("{}|{}|{}|{}|{}|{}|{}\n".format(datetime.utcnow(), self.host, start, sql,
                                                      status, error, time_taken))

class DBConnectionPool:
    """Handle connections to the DB manually rather than relying on MySQL connection pooling."""
    def __init__(self, pool_size, logfile, **kwargs):
        self.conn_args = kwargs
        self.logfile = logfile
        self.raise_on_warnings = kwargs.get('raise_on_warnings', False)
        self.connection_pool = []
        self.connections = 0
        self.max_pool = pool_size

    def _new_connection(self):
        """Create a new connection using *self.conn_args*."""
        return mysql.connector.connect(**self.conn_args)

    def get_connection(self):
        """
        Take a connection out of the pool. If the pool is exhausted, either create a new connection
        or raise a "Connection pool exhausted" exception if the maximum pool size has been reached.
        This prevents creation of too many open connections.
        """
        if len(self.connection_pool) > 0:
            cnx = self.connection_pool[0]
            self.connection_pool.pop(0)
        else:
            if self.connections < self.max_pool:
                cnx = self._new_connection()
                self.connections += 1
            else:
                raise DBConnectorException(logfile=self.logfile, msg="Connection pool exhausted.")
        return cnx

    def return_connection(self, cnx):
        """
        Return a connection to the pool. Incoming connections are tested and if they are no longer
        usable will be disgarded using *DBConnectionPool.close_connection*, which frees up the slot.
        """
        if isinstance(cnx, connection.MySQLConnection):
            if cnx.is_connected():
                self.connection_pool.append(cnx)
            else:
                self.close_connection(cnx)
        else:
            self.warning("Tried to return an object that is not connection.MySQLConnection object "
                         "to the pool...")
        return

    def close_connection(self, cnx):
        """Permanently close a connection and free up a slot in *self.connections*."""
        if isinstance(cnx, connection.MySQLConnection):
            try:
                cnx.close()
            except:
                self.warning("Failed to close connection...")
        else:
            self.warning("Tried to close a non-connection.MySQLConnection object...")
        self.connections -= 1
        return

    def close_all(self):
        """Close all connections when done with them for maximum DB efficiency."""
        for cnx in self.connection_pool:
            self.close_connection(cnx)
        if self.connections != 0:
            self.warning("Failed to account for all connections in DBConnectionPool.close_all()...")
        return

    def warning(self, msg):
        """
        Print a nicely formatted warning or raise as an error if *self.raise_on_warnings* is set to
        True.
        """
        msg = "WARNING! %s" % msg
        if self.raise_on_warnings:
            raise DBConnectorException(logfile=self.logfile, msg=msg)
        else:
            print(msg)
        return

class DBConnector:
    """
    A wrapper for the MySQL Python connector that provides additional resilience and functionality.

    Parameters
    ----------
    `mysql_defaults` : string
        Absolute path to the mysql options file, which must contain 'host', 'user', 'password' and
        'database' client options.
    `logfile` : string
        Absolute path to the file into which errors should be logged.
    `db_config` : dict
        Config for the DB connection if no mysql option file is passed. Must contain keys for
        'user', 'password', 'database' and 'host'.
    `session_tz` : string
        Optionally set the session timezone (useful if working with timestamps). Use any of the time
        zone names listed `here <https://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_.
        Defaults to "UTC".
    `use_pure` : boolean
        Whether to use the pure Python implementation of MySQL connector (True) or use the C
        extentsion (False). Default is True i.e. pure Python. N.B. C extension is much quicker when
        returning large datasets - see `Connector/Python C Extension Docs
        <https://dev.mysql.com/doc/connector-python/en/connector-python-cext.html>`_.
    `query_log` : string
        Optionally specify a log file where query stats will be logged.
    Notes
    -----
    You must either pass the mysql defaults file (which must contain all of the required
    parameters) or pass the db_config dict with keys: 'host', 'user', 'password', 'database'.
    Warnings
    --------
    Logging is currently not available.
    """
    def __init__(self, mysql_defaults=None, logfile=None, db_config=None, session_tz="UTC",
                 use_pure=True, query_log=None):
        default_timeout = 60
        pool_size = 1
        if mysql_defaults:
            self.db_config = {
                "option_files": mysql_defaults,
                "connection_timeout": default_timeout,
                "buffered": True,
                "get_warnings": True,
                "raise_on_warnings": False,
                "use_pure": use_pure,
            }
        else:
            self.db_config = {
                "user": db_config["user"],
                "password": db_config["password"],
                "database": db_config["database"],
                "host": db_config["host"],
                "connection_timeout": default_timeout,
                "buffered": True,
                "get_warnings": True,
                "raise_on_warnings": False,
                "use_pure": use_pure,
            }
        self.pool = DBConnectionPool(pool_size, logfile, **self.db_config)
        self.logfile = logfile
        self.excusable_errors = (
            #Can't connect to MySQL server on...
            errorcode.CR_CONN_HOST_ERROR,
            errorcode.CR_IPSOCK_ERROR,
            errorcode.CR_SERVER_GONE_ERROR,
            errorcode.CR_SERVER_LOST,
            errorcode.CR_SERVER_LOST_EXTENDED,
            #Too many connections
            errorcode.ER_CON_COUNT_ERROR,
            #Deadlock found when trying to get lock; try restarting transaction
            errorcode.ER_LOCK_DEADLOCK,
            #Server shutdown in progress
            errorcode.ER_SERVER_SHUTDOWN,
            #Lock wait timeout exceeded (maybe extend timeout?)
            errorcode.ER_LOCK_WAIT_TIMEOUT,
        )
        self.sleep_interval = 1
        self.cnx_retries = 10
        self.session_tz = session_tz
        self.query_log = DBConnectorLog(query_log, self.db_config)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close_connections()

    def _connect(self):
        """
        Open a connection to the database. Use connection pooling to reduce new connections.

        Returns
        -------
        mysql.connector.connection.MySQLConnection
            Database connection object.
        See Also
        --------
        `MySQL Connector Python Docs <https://dev.mysql.com/doc/connector-python/en/index.html>`_,
        `MySQL Connection Pooling Docs
        <https://dev.mysql.com/doc/connector-python/en/connector-python-connection-pooling.html>`_
        Notes
        -----
        If a connection pool is not yet opened, then calling a mysql.connector.connect() with the
        *pool_size* option will open a new pool and return a connection. If a connection pool with
        the same name is already open, then a connection will be returned from that pool.
        By not setting the pool_name parameter, the connect() function automatically generates the
        name, composed from whichever of the host, port, user, and database connection arguments are
        given.
        """
        cnx = None
        try:
            cnx = self.pool.get_connection()
            if self.session_tz is not None:
                cnx.time_zone = self.session_tz
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise DBConnectorException(self.logfile, "Something is wrong with the mysql "
                                           "username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                raise DBConnectorException(self.logfile, "The database does not exist.")
            elif err.errno in self.excusable_errors:
                print("MySQL Error... %s" % str(err))
                self.pool.return_connection(cnx)
            else:
                raise
        except:
            raise
        return cnx

    def _safe_connect(self):
        """Get a connection to the database with added resilience."""
        retries = 0
        test = False
        while not test and retries <= self.cnx_retries:
            cnx = self._connect()
            retries += 1
            try:
                test = cnx.is_connected()
            except:
                print("_connect() FAILED!! Retrying in %d seconds" % self.sleep_interval)
                self.pool.return_connection(cnx)
                test = False
                TIME.sleep(self.sleep_interval)
                self.sleep_interval *= 2
        if retries > self.cnx_retries:
            err = sys.exc_info()[0]
            raise DBConnectorException(self.logfile, "Failed to connect to the DB after %d "
                                       "retries. Latest error: %s" % (self.cnx_retries, str(err)))
        self.sleep_interval = 1
        return cnx

    def _safe_query(self, query_type, **kwargs):
        """
        Execute an SQL statement with added resilience.

        Parameters
        ----------
        `query_type` : DBConnector class method
            One of *DBConnector._select_query*, *DBConnector._proc_query*,
            *DBConnector._iud_query*.
        Returns
        -------
        list of lists
            As returned by the `MySQL Python Connector
            <https://dev.mysql.com/doc/connector-python/en/index.html>`__.
        Notes
        -----
        Any additional arguments required by the query methods must be supplied as *kwargs*.
        This method retries failed queries due to *excusable errors* an indefinitely with
        exponential back-off. Queries that fail due to failure to retrieve a connection also retry
        with exponential back-off, but only up to *self.cnx_retries* times.
        """
        success = False
        while not success:
            cnx = self._safe_connect()
            start = datetime.utcnow()
            try:
                result = query_type(cnx, **kwargs)
                success = True
                if self.query_log is not None:
                    time_taken = (datetime.utcnow() - start).total_seconds()
                    sql = kwargs.get('sqlquery', None)
                    self.query_log.log_query(sql, start, time_taken)
            except mysql.connector.Error as err:
                if err.errno in self.excusable_errors:
                    if self.query_log is not None:
                        time_taken = (datetime.utcnow() - start).total_seconds()
                        sql = kwargs.get('sqlquery', None)
                        self.query_log.log_query(sql, start, time_taken, 1, err)
                    self.pool.return_connection(cnx)
                    print("MySQL Error... %s" % str(err))
                    TIME.sleep(self.sleep_interval)
                    self.sleep_interval *= 2
                else:
                    self.pool.return_connection(cnx)
                    raise
            except:
                self.pool.return_connection(cnx)
                err = sys.exc_info()[0]
                raise DBConnectorException(self.logfile, "Encountered an error during mysql query "
                                                         "(%s)." % str(err))
        self.pool.return_connection(cnx)
        self.sleep_interval = 1
        return result

    def query(self, sqlquery, df=False):
        """
        Query the database using select (with resilience).

        Parameters
        ----------
        `sqlquery` : string
            SQL `select` statement to be executed.
        `df` : boolean
            Set to True to return query results as Pandas DataFrame. Column names will be extracted
            from the *sqlquery* and converted to lowercase (do not use "select * from").
        Returns
        -------
        list
            List of tuples, [(R1C1, R1C2, ...), (R2C1, R2C2, ...), ...].
            Length of list corresponds to N rows returned, length of tuples corresponds to columns
            selected.
        See Also
        --------
        MySQL Connector Python Docs: https://dev.mysql.com/doc/connector-python/en/index.html.
        Notes
        -----
        MySQL Connector Python is prone to dropped connections - this wrapper adds resilience by
        retrying.
        """
        return self._safe_query(self._select_query, sqlquery=sqlquery, df=df)

    def proc(self, proc, args):
        """Execute a MySQL procedure with resilience."""
        return self._safe_query(self._proc_query, proc=proc, args=args)

    def iud_query(self, sqlquery, data=None, size=1000):
        """Execute an insert/update/delete SQL statement with resilience."""
        return self._safe_query(self._iud_query, sqlquery=sqlquery, data=data, size=size)

    def close_connections(self):
        """Close all connections when done for optimal DB efficiency."""
        self.pool.close_all()

    @staticmethod
    def _safe_close(cnx):
        """Close a DB connection with resilience. DEPRECATED"""
        try:
            while cnx:
                cnx.close()
                while cnx.is_connected():
                    cnx.close()
        except (AttributeError, mysql.connector.errors.OperationalError):
            pass
        except:
            raise
        return

    @staticmethod
    def _select_query(cnx, **kwargs):
        """Execute a select query."""
        sqlquery = kwargs.get('sqlquery', None)
        df = kwargs.get('df', False)
        cursor = cnx.cursor()
        cursor.execute(sqlquery)
        result = cursor.fetchall()
        cursor.close()
        if df:
            from pandas import DataFrame
            import re
            col_regex = "(?<=^select)[a-zA-Z0-9_\s*(),`]+(?=from)"
            cols = [c.strip().split(" as ")[-1].strip().strip("`") for c in
                    re.findall(col_regex, sqlquery.lower())[0].strip().split(",")]
            return DataFrame(result, columns=cols)
        return result

    @staticmethod
    def _proc_query(cnx, **kwargs):
        """Execute a MySQL procedure."""
        proc = kwargs.get('proc', None)
        args = kwargs.get('args', None)
        cursor = cnx.cursor()
        result = []
        cursor.callproc(proc, args)
        for res in cursor.stored_results():
            result.append(res.fetchall())
        cursor.close()
        return result

    @staticmethod
    def _iud_query(cnx, **kwargs):
        """Execute an insert/update/delete SQL statement."""
        size = kwargs.get('size', 1000)
        data = kwargs.get('data', None)
        sqlquery = kwargs.get('sqlquery', None)
        cursor = cnx.cursor()
        if data is None:
            cursor.execute(sqlquery)
        else:
            for i in range(0, len(data), size):
                cursor.executemany(sqlquery, data[i:(i+size)])
        cnx.commit()
        affected = cursor.rowcount
        cursor.close()
        return affected
