import os
import traceback
import boto3
import pymysql
# import os
import requests
# import pymysqlpool
from boto3.dynamodb.conditions import Key
from .griiip_exeptions import SqlCursorNoneException
from .interfaces import IDataBaseClient
from .decorators import ifNotConnectDo, addTable


class DbPyMySQL(IDataBaseClient):
    MAX_RETRIES = 3
    is_conned = False

    def __init__(self, mySqlPool):  # self, host, user, passwd, dbname):
        self.getConnection = mySqlPool.get_connection
        self.conn = self.getConnection()  # None
        # self.is_conned = self._connect()

    def __del__(self):
        self.conn.close()

    def _is_connect(self):
        if not self.conn:
            return False
        return True

    def _connect(self):
        try:
            if self.conn:
                return True
            self.conn = self.getConnection  # pymysql.connect(host=self.host, user=self.user,
            # password=self.passwd, db=self.dbname)
            return True

        except Exception as e:
            print(f"Connect to MySQL Server Failed\n {e}")
            return False

    def __query(self, sql, **kwargs):
        cursor = None
        for i in range(self.MAX_RETRIES):
            try:
                if 'use_dict_cursor' in kwargs:
                    cursor = self.conn.cursor(pymysql.cursors.DictCursor)
                else:
                    cursor = self.conn.cursor()
                cursor.execute(sql)
                break

            except (pymysql.InterfaceError, pymysql.OperationalError) as e:
                print(f"COULD'T EXECUTE QUERY: {sql} \n RETRY ATTEMPT: {i + 1} \n {traceback.format_exc()}")
                cursor = None

        return cursor

    @ifNotConnectDo
    def commit(self):
        self.conn.commit()
        self.conn.close()

    @ifNotConnectDo
    def get(self, sql_cmd, **kwargs):
        """
        get = the select function
        Parameters
        ----------
        sql_cmd
        kwargs

        Returns
        -------

        """
        try:
            cursor = self.__query(sql_cmd, **kwargs)
            if cursor is None:
                raise SqlCursorNoneException(ops='query')

            if 'first' in kwargs and kwargs['first'] is True:
                return cursor.fetchone()

            return cursor.fetchall()

        except SqlCursorNoneException as cursorNone:
            print(cursorNone)
            return None

        except Exception as e:
            print(f"query filed err {e} \n {traceback.format_exc()}")
            return None

    @ifNotConnectDo
    def put(self, sql_cmd, **kwargs):
        """
        put = insert
        Parameters
        ----------
        sql_cmd
        kwargs

        Returns
        -------

        """
        try:
            cursor = self.__query(sql_cmd)
            if cursor is None:
                raise SqlCursorNoneException(ops='insert')

            if 'not_commit' not in kwargs:
                self.conn.commit()
                self.conn.close()

            return True

        except SqlCursorNoneException as cursorNone:
            print(cursorNone)
            raise cursorNone

        except Exception as e:
            print(f"query filed err {e} \n {traceback.format_exc()}")
            raise e

    @ifNotConnectDo
    def post(self, sql_cmd, **kwargs):
        """
        pust = update
        Parameters
        ----------
        sql_cmd
        kwargs

        Returns
        -------

        """

        is_post: bool = False
        try:
            cursor = self.__query(sql_cmd)
            if cursor is None:
                raise SqlCursorNoneException(ops='update')

            if 'not_commit' not in kwargs:
                self.conn.commit()
                self.conn.close()

            is_post = True

        except SqlCursorNoneException as cursorNone:
            print(cursorNone)
            raise cursorNone

        except Exception as e:
            print(f"query filed err {e} \n {traceback.format_exc()}")
            raise e

        finally:
            return is_post

    @ifNotConnectDo
    def delete(self, sql_cmd, **kwargs):
        """
        delete = delete
        Parameters
        ----------
        sql_cmd
        kwargs

        Returns
        -------

        """
        is_post: bool = False
        try:
            cursor = self.__query(sql_cmd)
            if cursor is None:
                raise SqlCursorNoneException(ops='delete')

            if 'not_commit' not in kwargs:
                self.conn.commit()
                self.conn.close()

            return True

        except SqlCursorNoneException as cursorNone:
            print(cursorNone)
            raise cursorNone

        except Exception as e:
            print(f"query filed err {e} \n {traceback.format_exc()}")
            raise e


class DbApiWrapper(IDataBaseClient):
    """
    ApiWrapper is client for data base that use api get away
    for crud ops
    """

    @classmethod
    def __init__(cls, **kwargs):
        cls.api_address = kwargs['api_address']
        cls.api_key = kwargs['api_key']

    @classmethod
    def get(cls, url, **kwargs):
        payload = {}
        if kwargs is not None:
            payload = kwargs
        return requests.get(cls.api_address + url, params=payload, headers={'x-api-key': str(cls.api_key)})

    @classmethod
    def put(cls, url, **kwargs):
        body = {}
        if 'json' in kwargs:
            body = kwargs['json']
        return requests.put(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def post(cls, url, **kwargs):
        body = {}
        if 'json' in kwargs:
            body = kwargs['json']
        return requests.post(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def delete(cls, url, **kwargs):
        body = kwargs
        return requests.delete(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def commit(cls, url, **kwargs):
        pass


class DynamoDb(IDataBaseClient):
    tables: {} = {}

    def __init__(self, tables=[]):
        self.dynamoDb = boto3.resource('dynamodb')
        if len(tables) <= 0:
            return
        for tableName in tables:
            self._addTable(tableName)

    def _addTable(self, tableName: str):
        if tableName in self.tables.keys():
            return
        self.tables[tableName] = self.dynamoDb.Table(tableName)

    def _getTable(self, key: str):
        return self.dynamoDb.Table(self.tables[key])

    @addTable(tables)
    def get(self, *, tableName, key, **kwargs) -> {}:
        """
        qouery items fom dynamo db
        :param tableName: the table to query from
        :param key: the primary key name of the table
        :param kwargs: aether argument to add to thew query
        :return: items that was found
        """
        _key = Key(key)
        if "eq" in kwargs:
            _keyConditionExpression = _key.eq(kwargs['eq'])
            del kwargs['eq']  # remove used params
        else:
            _keyConditionExpression = key

        table = self._getTable(tableName)
        kwargs['KeyConditionExpression'] = _keyConditionExpression  # add the condition to kwargs to send to dynamo

        ddb_response = table.query(**kwargs)
        return ddb_response["Items"] if "Items" in ddb_response else None

    @addTable(tables)
    def post(self, sql_cmd, **kwargs):
        # Todo need to be implement (update item method)
        pass

    @addTable(tables)
    def put(self, *, tableName: str, items: [], **kwargs):
        """
        insert items to specific table
        :param tableName: table name to insert to
        :param items: json array of items to insert
        :param kwargs: additional params (Not in use right now)
        :return: raise exception or return true
        """
        table = self._getTable(tableName)
        try:
            with table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            return batch
        except Exception as e:
            raise e

        return True

    @addTable(tables)
    def delete(self, *, tableName: str, key: str, **kwargs):
        table = self._getTable(tableName)
        try:
            table.delete_item(
                Key={
                    "lap_id": key
                }
            )
        except Exception as e:
            raise e
        # Todo need to be implement (delete item method)
        pass


"""
api: IDataBaseClient = DbApiWrapper(api_address=environ('griiip_api_url'), api_key=environ('griiip_api_key'))
sql: IDataBaseClient = DbPyMySQL()
ddb: IDataBaseClient = DynamoDb()
"""
