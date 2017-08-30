try:
    #py2
    from dbconnector import DBConnector
except:
    #py3+
    from dbconnector.dbconnector import DBConnector

__all__ = ["DBConnector"]
