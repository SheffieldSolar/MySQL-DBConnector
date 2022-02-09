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
from logging import info, debug, warning, error, critical, exception
import re
from typing import Optional

from pandas import DataFrame
import mysql.connector
from mysql.connector import errorcode, connection

class DBConnectorException(Exception):
    """An Exception specific to the DBConnector class."""
    def __init__(self, msg):
        try:
            caller_file = inspect.stack()[2][1]
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            caller_file = os.path.basename(__file__)
        error(msg)
        self.msg = f"{msg} (in '{caller_file}')"

    def __str__(self):
        return self.msg

class DBConnectorLog:
    """Record, read and analyse DBConnector stats."""
    def __init__(self, logfile, conn_args):
        self.logfile = logfile
        self.status = {0: "success", 1: "error", 2: "warning"}
        if "option_files" in conn_args:
            _, self.host = os.path.split(conn_args["option_files"])
        else:
            self.host = f"{conn_args['host']}.{conn_args['user']}.{conn_args['database']}"

    def log_query(self, sql, start, time_taken, status=0, err=None):
        """Append query details to the log file."""
        if self.logfile is None:
            return
        status = self.status[status]
        with open(self.logfile, "ab") as log:
            log.write(f"{datetime.utcnow()}|{self.host}|{start}|{sql}|{status}|{err}|"
                      f"{time_taken}\n")

class DBConnectionPool:
    """Handle connections to the DB manually rather than relying on MySQL connection pooling."""
    def __init__(self, connector_args: dict, pool_size: int):
        self.conn_args = connector_args
        self.connection_pool = []
        self.connections = 0
        self.max_pool = pool_size

    def _new_connection(self):
        """Create a new connection using *self.conn_args*."""
        return mysql.connector.connect(**self.conn_args)

    def _get_connection(self):
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
                raise DBConnectorException(msg="Connection pool exhausted.")
        return cnx

    def _return_connection(self, cnx):
        """
        Return a connection to the pool. Incoming connections are tested and if they are no longer
        usable will be disgarded using *DBConnectionPool._close_connection*, which frees up the
        slot.
        """
        if isinstance(cnx, connection.MySQLConnection) and cnx.is_connected():
            self.connection_pool.append(cnx)
        else:
            self._close_connection(cnx)
        return

    def _close_connection(self, cnx):
        """Permanently close a connection and free up a slot in *self.connections*."""
        if isinstance(cnx, connection.MySQLConnection):
            try:
                cnx.close()
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except:
                self.warning("Failed to close connection...")
        self.connections -= 1
        return

    def close_all(self):
        """Close all connections when done with them for maximum DB efficiency."""
        for cnx in self.connection_pool:
            self._close_connection(cnx)
        if self.connections != 0:
            self.warning("Failed to account for all connections in DBConnectionPool.close_all()")
        return

    def warning(self, msg: str):
        """
        Log a warning or raise as an error if *self.conn_args["raise_on_warnings"]* is set to True.
        """
        if self.conn_args["raise_on_warnings"]:
            raise DBConnectorException(msg=msg)
        else:
            warning(msg)
        return

class DBConnector:
    """
    A wrapper for the MySQL Python connector that provides additional resilience and functionality.

    Parameters
    ----------
    connector_args : dict
        A dictionary of connect args to pass to `mysql.connector.connect()`. The full list of args
        can be found `here <https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html>`_.
        Some args are set to default values which override those of the `mysql-connector-python`
        lib. These are:

        - `time_zone`: "UTC"
        - `connection_timeout`: 60
        - `buffered`: True
        - `get_warnings`: True
        - `raise_on_warnings`: False
        - `use_pure`: True
    query_log : string, optional
        Optionally specify a log file where query stats will be logged.
    pool_size : int, optional
        Optionally specify the maximum pool size (i.e. number of connections open at any time).
        Default is 1.
    cnx_retries : int, optional
        Optionally specify the maximum number of times to retry failed connections/queries.
        Default is 10.
    sleep_interval : int, optional
        Optionally specify time (in seconds) to sleep inbetween failed connections/queries which are
        retried. This code uses an exponentil back-off, so the sleep time will double after each
        failed connection / query - this arg determines the initial sleep time. Default is 1.

    Notes
    -----
    Some `connector_args` have no defaults and are always required: 'user', 'password', 'database'.
    """
    def __init__(
        self,
        connector_args: dict,
        query_log: Optional[str] = None,
        pool_size: int = 1,
        cnx_retries: int = 10,
        sleep_interval: int = 1
    ) -> None:
        self.connector_args = {
            "time_zone": "UTC",
            "connection_timeout": 60,
            "buffered": True,
            "get_warnings": True,
            "raise_on_warnings": False,
            "use_pure": True,
        }
        self.connector_args.update(connector_args)
        self.pool = DBConnectionPool(self.connector_args, pool_size)
        self.retry_errors = (
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
        debug(f"config: {self._redacted_connector_args()}")
        self.sleep_interval = sleep_interval
        self.cnx_retries = cnx_retries
        self.query_log = DBConnectorLog(query_log, self.connector_args)
        self._test_query()

    def __enter__(self):
        """Enter the context manager. Test the connection and log a debug message."""
        return self

    def __exit__(self, *args):
        self.close_connections()

    def _test_query(self):
        """
        Create a new connection and submit a test query to see if the connection has been
        successful. Used to ensure any failure to connection errors occur at the point of creating
        the DBConnector instance rather than waiting until the first query is made.
        """
        cnx_retries = self.cnx_retries
        self.cnx_retries = 3
        connection_timeout = self.connector_args["connection_timeout"]
        self.pool.conn_args["connection_timeout"] = 5
        debug("submitting test query")
        try:
            self.query("SELECT 1;")
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            debug("test query failed")
            raise
        debug("test query successful")
        self.cnx_retries = cnx_retries
        self.pool.conn_args["connection_timeout"] = connection_timeout
        return

    def _redacted_connector_args(self):
        return {k: v if k!="password" else "REDACTED" for k, v in self.connector_args.items()}

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
            cnx = self.pool._get_connection()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise DBConnectorException("Something is wrong with the mysql username or "
                                           "password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                raise DBConnectorException("The database does not exist.")
            elif err.errno in self.retry_errors:
                warning(f"MySQL Connector Error: {err}")
                self.pool._return_connection(cnx)
                return None
            else:
                raise
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            raise
        return cnx

    def _connect_retry(self, cnx_retries=1):
        """Get a connection to the database with optional retries."""
        retries = 0
        test = False
        sleep_interval = self.sleep_interval
        while not test:
            cnx = self._connect()
            retries += 1
            if cnx is None:
                test = False
            else:
                test = cnx.is_connected()
            if not test:
                if retries < cnx_retries:
                    warning(f"Connection failed - retrying in {self.sleep_interval} seconds")
                    self.pool._return_connection(cnx)
                    test = False
                    TIME.sleep(sleep_interval)
                    sleep_interval *= 2
                else:
                    #import pdb; pdb.set_trace()
                    raise DBConnectorException(f"Failed to connect to the DB after {cnx_retries} "
                                               f"retries.")
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
        This method retries failed queries due to *retry_errors* with exponential back-off.
        Queries that fail due to failure to retrieve a connection also retry
        with exponential back-off, but only up to *self.cnx_retries* times.
        """
        success = False
        sleep_interval = self.sleep_interval
        while not success:
            cnx = self._connect_retry(cnx_retries=self.cnx_retries)
            start = datetime.utcnow()
            try:
                result = query_type(cnx, **kwargs)
                success = True
                if self.query_log is not None:
                    time_taken = (datetime.utcnow() - start).total_seconds()
                    sql = kwargs.get("sqlquery", None)
                    self.query_log.log_query(sql, start, time_taken)
            except mysql.connector.Error as err:
                if err.errno in self.retry_errors:
                    if self.query_log is not None:
                        time_taken = (datetime.utcnow() - start).total_seconds()
                        sql = kwargs.get("sqlquery", None)
                        self.query_log.log_query(sql, start, time_taken, 1, err)
                    self.pool._return_connection(cnx)
                    warning(f"MySQL Error: {err}")
                    TIME.sleep(sleep_interval)
                    sleep_interval *= 2
                else:
                    self.pool._return_connection(cnx)
                    raise
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except:
                self.pool._return_connection(cnx)
                err = sys.exc_info()[0]
                raise DBConnectorException(f"Encountered an error during MySQL query ({err}).")
        self.pool._return_connection(cnx)
        return result

    def close_connections(self):
        """Close all connections when finished for optimal DB efficiency."""
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
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            raise
        return

    @staticmethod
    def _select_query(cnx, **kwargs):
        """Execute a select query."""
        sqlquery = kwargs.get("sqlquery", None)
        debug(f"select query: {sqlquery}")
        df = kwargs.get("df", False)
        cursor = cnx.cursor()
        cursor.execute(sqlquery)
        result = cursor.fetchall()
        cursor.close()
        if df:
            debug("converting query result to dataframe")
            col_regex = "(?<=^select)[a-zA-Z0-9_\s*(),`]+(?=from)"
            cols = [c.strip().split(" as ")[-1].strip().strip("`") for c in
                    re.findall(col_regex, sqlquery.lower())[0].strip().split(",")]
            return DataFrame(result, columns=cols)
        return result

    @staticmethod
    def _proc_query(cnx, **kwargs):
        """Execute a MySQL procedure."""
        proc = kwargs.get("proc", None)
        proc_args = kwargs.get("proc_args", None)
        debug(f"procedure call: call {proc}({', '.join(map(str, proc_args))})")
        cursor = cnx.cursor()
        result = []
        cursor.callproc(proc, proc_args)
        for res in cursor.stored_results():
            result.append(res.fetchall())
        cursor.close()
        return result

    @staticmethod
    def _iud_query(cnx, **kwargs):
        """Execute an insert/update/delete SQL statement."""
        chunk_size = kwargs.get("chunk_size", 1000)
        data = kwargs.get("data", None)
        sqlquery = kwargs.get("sqlquery", None)
        debug(f"insert/update/delete query: {sqlquery}")
        cursor = cnx.cursor()
        if data is None:
            cursor.execute(sqlquery)
        else:
            for i in range(0, len(data), chunk_size):
                cursor.executemany(sqlquery, data[i:(i+chunk_size)])
        cnx.commit()
        affected = cursor.rowcount
        cursor.close()
        return affected

    def query(self, sqlquery: str, df: bool = False):
        """
        Query the database using a select statement.

        Parameters
        ----------
        sqlquery : string
            SQL statement to be executed.
        df : boolean, optional
            Set to True to return query results as Pandas DataFrame. Column names will be extracted
            from the *sqlquery* and converted to lowercase (do not use "select * from").

        Returns
        -------
        list *OR* Pandas DataFrame
            If `df=False`, returns a list of tuples, [(R1C1, R1C2, ...), (R2C1, R2C2, ...), ...].
            Length of list corresponds to N rows returned, length of tuples corresponds to columns
            selected.

            If `df=True`, returns a Pandas DataFrame containing the columns from the `sqlquery` and
            any rows returned.
        """
        return self._safe_query(self._select_query, sqlquery=sqlquery, df=df)

    def proc(self, proc: str, proc_args: list):
        """
        Execute a MySQL procedure.
        
        Parameters
        ----------
        proc : string
            The name of the MySQL procedure.
        proc_args : list
            A list of arguments to pass to the procedure.

        Returns
        -------
        list
            A list of stored results, see `here <https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-stored-results.html>`_.
        """
        return self._safe_query(self._proc_query, proc=proc, proc_args=proc_args)

    def iud_query(self, sqlquery: str, data: Optional[list[list]] = None, chunk_size: int = 1000):
        """
        Execute an insert/update/delete SQL statement.
        
        Parameters
        ----------
        sqlquery : string
            SQL statement to be executed. Optionally use '%s' placeholders for values in conjunction
            with the `data` arg.
        data : list of lists, optional
            A list of lists containing any values that should be used to populate '%s' placeholders
            in the VALUES section of `sqlquery`.
        chunk_size : int, optional
            Optionally break the insert up into chunks of this size when using `data` to pass
            values. This can be useful if inserts to the DB are slow as shorter lived SQL queries
            are generally preferred.

        Returns
        -------
        int
            The number of rows affected.
        """
        return self._safe_query(self._iud_query, sqlquery=sqlquery, data=data,
                                chunk_size=chunk_size)
