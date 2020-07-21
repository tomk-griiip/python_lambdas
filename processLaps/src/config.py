from src.lambda_utils import *
import src.griiip_const as const
import os
import boto3

ssm = boto3.client('ssm')
# -------------- static method's to help building the beans objects ---------------#

"""
return the lap distance
"""


def get_lap_distance(lapQuads: []):
    if len(lapQuads) <= 0:
        return 0
    return max(lapQuads, key=lambda item: item.distance).distance


"""
return the lap time 
"""


def get_lap_time(lapQuads: []):
    if len(lapQuads) <= 0:
        return 0
    return format_seconds_to_hhmmss(max(lapQuads, key=lambda item: item.lapTime).lapTime)


"""
return the lap finish long 
"""


def get_lap_long(lapQuads: []):
    if len(lapQuads) <= 0:
        return 0
    return max(lapQuads, key=lambda item: item.gpsLong).gpsLong


"""
return the lap finish lat 
"""


def get_lap_lat(lapQuads: []):
    if len(lapQuads) <= 0:
        return 0
    return max(lapQuads, key=lambda item: item.gpsLat).gpsLat


def get_lap_start_date(lapQuads: []):
    if len(lapQuads) <= 0:
        return None
    lapId = lapQuads[0].lapName
    return calc_lap_start_date(lap_name=lapId, yearPrefix=os.environ['year_prefix'])


def get_acc_comb(lapQuads: []):
    count_legit_rows, sum_lat_acc, sum_long_acc = 0, 0, 0

    for quad in lapQuads:
        if quad.speed <= 30:
            continue

        count_legit_rows += 1
        sum_lat_acc += abs(quad.latAcc)
        sum_long_acc += abs(quad.longAcc)

    return calculate_acc_comb(sum_lat_acc, sum_long_acc, count_legit_rows)


"""
@:param run data rows
@:return the amount of time that driver was in low speed in this lap

"""


def get_low_speed_time(lapQuads: []):
    count_legit_rows = 0
    temp_lap_time = 0
    biggest_lap_time_interval = 0  # the amount of time that driver was in low speed in this lap
    for quad in lapQuads:
        if quad.speed > 30:
            temp_lap_time = 0
            continue
        temp_lap_time = 0 if temp_lap_time == 0 else quad.lapTime

        if quad.lapTime - temp_lap_time > biggest_lap_time_interval:
            biggest_lap_time_interval = quad.lapTime - temp_lap_time

    return biggest_lap_time_interval


"""
@:class  Config hold inside dicts that config 
the consumer services and its class 
"""


class Config:
    # dict that hold all the fields in the Lap object
    driverLapFieldDict: dict = {
        "lapName": "",
        "UserId": '',
        "TrackId": '',
        "CarId": '',
        "distance": 0,
        "lapTime": 0,
        "_lat": 0,
        "_long": 0,
        "_length": 0,
        "lapStartDate": '',
        "accCombinedAvg": 0,
        "_low_speed_time": 0,  # gets the biggest time interval where the car was under 30 kph
    }

    # fieldsFromSqsMessage is dictionary that map the fields that we get from the sws message
    # to the Lap object fields
    # if changing the sqs message need to change this config map

    fieldsFromSqsMessage: dict = {
        "lapName": "lapId",
        "UserId": "userId",
        "TrackId": "trackId",
        "CarId": "carId"
    }

    # dict that hold the field's that need to calculate by a function
    # the function need to be implemented in this file
    # and each field in this dict need to have function
    driverLapFuncToCalcField: dict = {
        "_distance": get_lap_distance,
        "lapTime": get_lap_time,
        "_lat": get_lap_lat,
        "_long": get_lap_long,
        "lapStartDate": get_lap_start_date,
        "accCombinedAvg": get_acc_comb,
        "__low_speed_time": get_low_speed_time

    }

    def __init__(self):
        self.parameters = {}
        nextToken = ' '
        env = os.environ['Env']
        while nextToken is not None:
            _parameters = ssm.get_parameters_by_path(Path=f"/{env}", Recursive=True, NextToken=nextToken)
            nextToken = _parameters.get('NextToken', None)
            for p in _parameters['Parameters']:
                if not p['Name'].startswith(f'/{env}/processLaps/'):
                    continue
                key = p['Name'][len(f'/{env}/processLaps/'):]
                value = p['Value']
                self.parameters[key] = value


config = Config()
print('end')
