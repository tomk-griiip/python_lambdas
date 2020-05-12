"""
in the beans file there are only
class that hold ( beans ) data in the form of class
all the class in this file are only for data structure purposes

"""
from json import JSONEncoder
# import lambda_utils
from lambda_utils import *
from datetime import datetime
import os
from api_wrapper import IApiWrapper
from griiip_exeptions import TracksException

"""

@LapBean hold ReceivedLap and the lapId that need to be generate

"""


class LapBean(object):
    """
    @ReceivedLap class that hold the info
    that process laps received from api_gateway_to_process_laps sqs queue
    """

    class ReceivedLap(object):

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
    @return {car_id}{day}{month}{year}{hour}{minutes}{second}{lap_number}
                3     2     2       2   2       2       2       3  
    """

    def create_lap_id(self):
        start_date: datetime = datetime.utcfromtimestamp(self.lap.lapStartTime)
        day, month, year, hour, minutes, second = get_day_month_year(start_date)
        car_id: str = int_to_tree_digit_string(self.lap.carId)
        lap_number = "000"  # temp value because we dont know yet what lap is it
        return f"{car_id}{day}{month}{year}{hour}{minutes}{second}{lap_number}"


"""
@:type RunDataRow is bean object that hold 
row from `driverlapsrundata` Table fro mysql RDS 
"""


class RunDataRow(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)


"""
class that need to be implanted in order to create const property in an object
"""


class Constant(object):
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


"""
@:type Lap represent full lap object 
"""


class Lap(Constant):
    """
    constant property's the value for them is from template.yaml file
    do not change change them from code !!!!
    """
    MAX_ACC_PERCENT: float = environ('MAX_ACC_PERCENT', float)
    FULL_LAP_FLOOR: float = environ('FULL_LAP_FLOOR', float)
    FULL_LAP_CELL = environ('FULL_LAP_CELL')
    PART_LAP_FLOOR: float = environ('PART_LAP_FLOOR', float)
    _trackGpsLength: float = 0.0
    _columns_to_update: dict = {}
    _classification: str = None

    def __init__(self, runData: [], funcToField: dict, **entries) -> object:
        self.__dict__.update(entries)  # create the object fields according to configuration
        # create array of RunDataRow bean object
        self.__lap_quads: [] = [RunDataRow(**runData[i]) for i in range(len(runData))]

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

    """
    @:param an api wrapper class (class that communicate with the RDS)
    @:return set the gps length of the track  
    """

    def set_track_length(self, ApiWrapper_cls: IApiWrapper):
        end_point: str = f"/trackmap/{self.TrackId}"
        length: float = None
        try:
            length: float = ApiWrapper_cls.get(end_point).json()['gpsLength']

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


class RunDataRowEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__
