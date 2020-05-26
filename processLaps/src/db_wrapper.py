import traceback
import boto3
import pymysql
import os
import requests

from boto3.dynamodb.conditions import Key
from abc import ABC
from griiip_const import net, errorMessages
from griiip_exeptions import RunDataException, ApiException, CantConnectToDbException, SqlCursorNoneException
from interfaces import IDataBaseClient, IDataBase
from lambda_utils import environ
from decorators import ifNotConnectDo, addTable


class DbPyMySQL(IDataBaseClient):

    def __init__(self, host, user, passwd, dbname):
        self.coon = None
        self.host = host
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.is_conned = self._connect()

    def __del__(self):
        self.disconnect_to_mysql()
        print(f"disconnect_to_mysql() success, host={self.host}, user={self.user}, db={self.dbname}")

    def _connect(self):
        try:
            if self.is_conned:
                return True
            self.coon = pymysql.connect(host=self.host, user=self.user, password=self.passwd, db=self.dbname)
            return True

        except Exception as e:
            print(f"Connect to MySQL Server Failed, host={self.host}, user={self.user}, db={self.dbname} \n {e}")
            return False

    def disconnect_to_mysql(self):
        try:
            if self.is_conned and self.coon is not None:
                self.coon.close()
                self.coon = None
                self.is_conned = False

        except Exception as e:
            print(f"Disconnect to MySQL Server Failed, errmsg = {e}")

    def __query(self, sql, use_dict_cursor=False):
        cursor = None
        for i in range(self.MAX_RETRIES):
            try:
                if use_dict_cursor:
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
        self.coon.commit()

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
            cursor = self.__query(sql_cmd)
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
        is_put: bool = False
        try:
            cursor = self.__query(sql_cmd)
            if cursor is None:
                raise SqlCursorNoneException(ops='insert')

            if 'commit' in kwargs:
                self.coon.commit()

            is_put = True

        except SqlCursorNoneException as cursorNone:
            print(cursorNone)

        except Exception as e:
            print(f"query filed err {e} \n {traceback.format_exc()}")

        finally:
            return is_put

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

            if 'commit' in kwargs:
                self.coon.commit()

            is_post = True

        except SqlCursorNoneException as cursorNone:
            print(cursorNone)

        except Exception as e:
            print(f"query filed err {e} \n {traceback.format_exc()}")

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

            if 'commit' in kwargs:
                self.coon.commit()

            is_post = True

        except SqlCursorNoneException as cursorNone:
            print(cursorNone)

        except Exception as e:
            print(f"query filed err {e} \n {traceback.format_exc()}")

        finally:
            return is_post


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
        body = kwargs
        return requests.put(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def post(cls, url, **kwargs):
        body = kwargs
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

        except Exception as e:
            raise e

        return True

    @addTable(tables)
    def delete(self, sql_cmd, **kwargs):
        # Todo need to be implement (delete item method)
        pass


api: IDataBaseClient = DbApiWrapper(api_address=environ('griiip_api_url'), api_key=environ('griiip_api_key'))

sql: IDataBaseClient = DbPyMySQL(host=environ("my_sql_host"),
                                 user=environ("my_sql_user"),
                                 passwd=environ("my_sql_pass"),
                                 dbname=environ("my_sql_db")
                                 )
ddb: IDataBaseClient = DynamoDb()
