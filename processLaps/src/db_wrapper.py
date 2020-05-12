from api_wrapper import *
from griiip_const import net, errorMessages
from griiip_exeptions import RunDataException, ApiException
from interfaces import IApiWrapper, IDb
from lambda_utils import environ
import pymysql


class DbPyMySQL(IDb):
    def __init__(self, host, user, passwd, dbname):

        self.dbHandle = None
        self.host = host
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.bConned = self._connect()

    def __del__(self):
        self.disconnect_to_mysql()
        print(f"disconnect_to_mysql() success, host={self.host}, user={self.user}, db={self.dbname}")

    def _connect(self):
        try:
            if self.bConned:
                return True
            self.dbHandle = pymysql.connect(host=self.host, user=self.user, password=self.passwd, db=self.dbname)
            return True

        except Exception as e:
            print(f"Connect to MySQL Server Failed, host=%s, user=%s, db=%s, port=%u, errmsg=%s")
            return False

    def disconnect_to_mysql(self):
        try:
            if self.bConned and self.dbHandle is not None:
                self.dbHandle.close()
                self.dbHandle = None
                self.bConned = False

        except Exception as e:
            print(f"Disconnect to MySQL Server Failed, errmsg={e}")

    def query(self, sql_cmd):
        pass

    def insert(self, sql_cmd):
        pass

    def update(self, sql_cmd):
        pass

    def delete(self, sql_cmd):
        pass

    def getRunData(self, lapId, **payload) -> []:
        pass

    def updateDriverLap(self, columns_to_update: {}, lap_name: str) -> bool:
        pass


class DbWithApiGetAway(IDb):

    def __init__(self, *, apiWrapper: IApiWrapper):
        if not isinstance(apiWrapper, IApiWrapper):
            raise InterfaceImplementationException("IApiWrapper")
        self.apiWrapper = apiWrapper

    def getRunData(self, lapId, **payload) -> []:
        """
        function that get all the runData of the lap By lapId
        from 'driverlapsrundata' Table in RDS
        Parameters
        ----------
        lapId
        limit
        page

        Returns
        -------
        array of type RunDataRow each object in the array is one record
        of the lapRunData
        """
        # call API to get runData
        payload['lapName'] = lapId
        runData: dict = self.apiWrapper.get(net.RUNDATA_URL, params=payload).json()['data']

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
            print(f"FOUND {num_dist_glit} BAD ROWS FOR LAP {lapId}"
                  f"\nLAP FIRST ROWS DISTANCE IS BIGGER THEN THE NEXT ROWS")

        return runData[num_dist_glit:]  # remove the rows with the distance glitches in the

    def updateDriverLap(self, columns_to_update: {}, lap_name: str) -> bool:
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
            res = self.apiWrapper.put(net.UPDATE_DRIVER_LAP_URL, json={**columns_to_update, "lapName": lap_name})

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

    def query(self, sql_cmd):
        pass

    def insert(self, sql_cmd):
        pass

    def update(self, sql_cmd):
        pass

    def delete(self, sql_cmd):
        pass


api = ApiWrapper(api_address=environ('griiip_api_url'), api_key=environ('griiip_api_key'))

db_api = DbWithApiGetAway(apiWrapper=api)
