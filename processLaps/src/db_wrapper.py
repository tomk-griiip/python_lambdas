import traceback
from abc import ABC

from api_wrapper import *
from griiip_const import net, errorMessages
from griiip_exeptions import RunDataException, ApiException, CantConnectToDbException, SqlCursorNoneException
from interfaces import IDataBaseClient, IDataBase
from lambda_utils import environ
import pymysql
from decorators import ifNotConnectDo


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


class DbWithApiGetAway(IDataBase):

    def putLap(self, insert, **kwargs):
        pass

    def commit(self, **kwargs):
        pass

    def __init__(self, *, dbClient: IDataBaseClient):
        if not isinstance(dbClient, IDataBaseClient):
            raise InterfaceImplementationException("IApiWrapper")
        self.dbClient = dbClient

    def getRunData(self, query, **kwargs) -> []:
        """
        function that get all the runData of the lap By lapId
        from 'driverlapsrundata' Table in RDS
        Parameters
        ----------
        query

        Returns
        -------
        array of type RunDataRow each object in the array is one record
        of the lapRunData
        """
        # call API to get runData
        runData: dict = self.dbClient.get(query, **kwargs).json()['data']

        if len(runData) == 0:
            raise RunDataException

        # some times the first rows is mistaken distance data
        # and need to remove them from the run data ro
        def removed_first_bad_distance_rows() -> int:
            glitches, total_rows, g = 0, len(runData), 0
            for row_id in range(total_rows - 1):
                # In this case, the row 'distance' value is bigger then the next row 'distance' value.
                if runData[row_id]['distance'] > runData[row_id + 1]['distance']:
                    glitches += 1
                else:
                    break
            return glitches

        # the number of glitches in thr beginning of the lap
        num_dist_glit: int = removed_first_bad_distance_rows()
        if num_dist_glit > 0:
            print(f"FOUND {num_dist_glit} BAD ROWS FOR LAP {kwargs['lapName']}"
                  f"\nLAP FIRST ROWS DISTANCE IS BIGGER THEN THE NEXT ROWS")

        return runData[num_dist_glit:]  # remove the rows with the distance glitches in the

    def updateDriverLap(self, update, **kwargs) -> bool:
        """
        Parameters
        ----------
        columns_to_update column withe their value to be update
        lap_name the lap to update

        Returns
        -------
        True for SUCCESS and False for FAILURE

        """
        try:
            columns_to_update, lapName = kwargs['columns_to_update'], kwargs['lap_name']
            res = self.dbClient.put(update, json={**columns_to_update, "lapName": lapName})

        except KeyError as ke:
            print(f'kwargs missing argument \n {ke}')
            return net.FAILURE

        except ApiException as api_e:
            print(f"db Api Exception : {api_e}")
            return net.FAILURE

        except Exception as e:
            print(f"DB Exception : {e}")
            return net.FAILURE

        if res.status_code == net.OK:
            return net.SUCCESS
        else:
            return net.FAILURE  # Consider raising exception instead


api = ApiWrapper(api_address=environ('griiip_api_url'), api_key=environ('griiip_api_key'))

db_api = DbWithApiGetAway(dbClient=api)
