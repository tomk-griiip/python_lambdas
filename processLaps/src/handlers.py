"""
in the beans file there are only
class that hold ( beans ) data in the form of class
all the class in this file are only for data structure purposes

"""
from json import JSONEncoder
# import lambda_utils
from src.griiip_const import net
from src.lambda_utils import *
from datetime import datetime
import os
from src.interfaces import IDataBase, IDataBaseClient
from src.griiip_exeptions import TracksException, RunDataException, ApiException
from . import logger
from .config import config as conf


class LapBean(object):
    """

    @LapBean hold ReceivedLap and the lapId that need to be generate

    """

    class ReceivedLap(object):
        """
        @ReceivedLap class that hold the info
        that process laps received from api_gateway_to_process_laps sqs queue
        """

        def __init__(self, *, record: dict):
            self.trackId = int(record['trackId'])
            self.carId = str(record['carId'])
            self.userId = str(record['userId'])
            self.lapStartTime = int(record['lapStartTime'])

    def __init__(self, *, record: dict):
        self.lap = self.ReceivedLap(record=record)
        self.lapId: str = self.create_lap_id()
        self.lap.lapStartTime: datetime = datetime.utcfromtimestamp(self.lap.lapStartTime)

    """
    @create_lap_id function generate the lap id 
    3 first digit is care id
    2 digit day
    2 digit month
    2 digit year
    2 digit hour
    2 digit minutes
    2 digit second
    3 digit lap number  
    @return {car_id}{month}{day}{year}{hour}{minutes}{second}{lap_number}
            n sefix    2     2       2   2       2       2       3  
    """

    def create_lap_id(self):
        start_date: datetime = datetime.utcfromtimestamp(self.lap.lapStartTime)
        day, month, year, hour, minutes, second = get_day_month_year(start_date)
        # dont need to parse car number to tree digit string
        car_id: str = self.lap.carId  # int_to_tree_digit_string(self.lap.carId)
        lap_number = "000"  # temp value because we dont know yet what lap is it
        return f"{car_id}{month}{day}{year}{hour}{minutes}{second}{lap_number}"


class RunDataRow(object):
    """
    @:type RunDataRow is bean object that hold
    row from `driverlapsrundata` Table fro mysql RDS

    """

    def __init__(self, **entries):
        self.__dict__.update(entries)


class RunData(object):
    @staticmethod
    def getRunData(db: IDataBaseClient, **kwargs) -> []:
        """
        function that get all the runData of the lap By lapId
        from 'driverlapsrundata' Table in RDS
        Parameters
        ----------
        db
        query
        kwargs

        Returns
        -------
        array of type RunDataRow each object in the array is one record
        of the lapRunData
        """
        # call API to get runData
        _http_res = db.get(net.RUNDATA_URL, **kwargs)
        if _http_res.status_code != 200:
            logger.error(f"{_http_res.status_code}: {_http_res.text}")
            return []

        runData: dict = _http_res.json()['data']

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
            logger.warning(f"FOUND {num_dist_glit} BAD ROWS FOR LAP {kwargs['lapName']}"
                           f"\nLAP FIRST ROWS DISTANCE IS BIGGER THEN THE NEXT ROWS")

        return runData[num_dist_glit:]  # remove the rows with the distance glitches in the


class Constant(object):
    """
    class that need to be implanted in order to create const property in an object
    """
    _const_list: [] = []  # list that hold all the constance of the class

    def set_const_list(self, _lst: []):
        self._const_list = _lst

    def get_const_list(self) -> []:
        return self._const_list

    def __setattr__(self, key, value):
        if key in self._const_list:  # if key is in the constant list do net allow to set it
            raise TypeError
        else:
            self.__dict__[key] = value


class Lap(Constant):
    """
    @:type Lap represent full lap object
    """

    # constant property's the value for them is from template.yaml file
    # do not change change them from code !!!!

    MAX_ACC_PERCENT: float = float(conf.parameters['mAXACCPercent'])
    FULL_LAP_FLOOR: float = float(conf.parameters['FullLapFloor'])
    FULL_LAP_CELL: float = float(conf.parameters['FullLapCell'])
    PART_LAP_FLOOR: float = float(conf.parameters['PartLapFloor'])
    _trackGpsLength: float = 0.0
    _columns_to_update: dict = {}
    _classification: str = None

    def __init__(self, runData: [], funcToField: dict, db: IDataBaseClient, **entries) -> object:
        self.__dict__.update(entries)  # create the object fields according to configuration
        # create array of RunDataRow bean object
        self.__lap_quads: [] = [RunDataRow(**runData[i]) for i in range(len(runData))]
        self._db = db

        # calculate fields value by functions from configuration
        for key, value in funcToField.items():
            valToSet = value(self.__lap_quads)
            setattr(self, key, valToSet)

        # insert to _columns_to_update all fields that are public
        # public fields in this class mean that they are fields that need
        # to be insert to mysql RDS
        _fields = [f for f in dir(self)
                   if
                   not f.startswith('__') and not f.startswith('_')  # not object standard prop     f not private prop
                   and not f.isupper() and not callable(getattr(self, f))]  # f not constant     f not function
        # for each field in _fields insert it to the _columns_to_update dict
        [self._columns_to_update.update({f: getattr(self, f)}) for f in _fields]
        # init the constant property in order that no aether class will override  them
        self.set_const_list(['MAX_ACC_PERCENT', 'FULL_LAP_FLOOR', 'FULL_LAP_CELL', 'PART_LAP_FLOOR'])

    def setColumnsToUpdate(self, d: {}):  # marge new dict to columns to update dict
        self._columns_to_update = {**self._columns_to_update, **d}

    def getColumnToUpdate(self) -> {}:
        return self._columns_to_update

    def get_classification(self):
        return self._classification

    def set_classification(self, classification: str):
        self._classification = self._columns_to_update['classification'] = classification

    def set_track_length(self):
        """
        set the gps track length from mysql track table
        Returns
        -------

        """
        length: float = None
        try:
            _http_res = self._db.get(f"{net.TRACK_MAP}{self.TrackId}")

            if _http_res.status_code != 200:
                length: float = 0.0
                logger.warning(f"{_http_res.status_code}: {_http_res.text}\n track gps length is 0")

            else:
                length: float = _http_res.json()['gpsLength']

        except Exception:
            pass

        if length is None:
            raise TracksException(self.TrackId)

        self._trackGpsLength = float(length) / 1000

    def getLapQuads(self) -> []:
        return self.__lap_quads

    def getLapName(self) -> str:
        lapName: str = None
        try:
            lapName = self.lapName

        except (ValueError, TypeError) as etv:
            raise etv

        except Exception as e:
            raise e

        finally:
            return lapName

    def updateDriverLap(self) -> bool:
        """
        update driverLaps table
        Returns
        -------
        True for SUCCESS and False for FAILURE
        """
        try:
            res = self._db.put(net.UPDATE_DRIVER_LAP_URL, json={**self._columns_to_update})

        except KeyError as ke:
            logger.error(f'kwargs missing argument \n {ke}')
            return net.FAILURE

        except ApiException as api_e:
            logger.error(f"db Api Exception : {api_e}")
            return net.FAILURE

        except Exception as e:
            logger.error(f"DB Exception : {e}")
            return net.FAILURE

        if res.status_code == net.OK:
            return net.SUCCESS
        else:
            return net.FAILURE  # Consider raising exception instead


class RunDataRowEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__
