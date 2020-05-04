from builtins import type

import math
import time
from math import log10
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
# import beans as beans
# from beans import Lap, RunDataRowEncoder
import os
import json
import asyncio
import griiip_const as const
dynamoDb = boto3.resource('dynamodb')
lambdaClient = boto3.client('lambda')

"""
wrap the os.environ function with try except
for safer local/global/system variable query 
"""


def environ(key: str, _type=None):
    try:
        if _type is None:
            return os.environ[key]

        return _type(os.environ[key])

    except KeyError:
        return None


"""
convert the number to 3 digit string that represent number
1 return '001'
12 return '012'
124 return '124'
0 return '000'
and so on
"""


def int_to_tree_digit_string(num) -> str:
    if type(num) is not str:
        num = str(num)
    n: int = len(num)
    if n == 1:
        return f"00{num}"
    elif n == 2:
        return f"0{num}"
    return num


"""
@:parameter datetime
@:returns  day, month, year, hour, minutes, second
"""


def get_day_month_year(date: datetime) -> ():
    d, m, y, h, mint, sec = date.day, date.month, date.year, date.hour, \
                            date.minute, date.second

    day: str = str(d) if int_length(d) == 2 else f"0{d}"
    month: str = str(m) if int_length(m) == 2 else f"0{m}"
    year: str = str(abs(y) % 100)
    hour: str = str(h) if int_length(h) == 2 else f"0{h}"
    minutes: str = str(mint) if int_length(mint) == 2 else f"0{mint}"
    second: str = str(sec) if int_length(sec) == 2 else f"0{sec}"
    return day, month, year, hour, minutes, second


"""
@:parameter n = number (integer)
@:return the number of digits in n
n = 12 return 2
n = 9 return 1
n = 0 return 1 and so on
"""


def int_length(n) -> int:
    if n < 0:
        return int(log10(-n)) + 2

    elif n == 0:
        return 1

    return int(log10(n)) + 1


def format_seconds_to_hhmmss(seconds):
    hours = int(seconds // (60 * 60))
    seconds %= (60 * 60)
    minutes = int(seconds // 60)
    seconds %= 60
    milliseconds = str(float(seconds % 1))
    milliseconds = "{:0<2.3}".format(milliseconds[2:])
    return_time = "%s:%s:%s.%s" \
                  % (str(hours).zfill(2), str(minutes).zfill(2), str(int(seconds)).zfill(2), milliseconds)
    return return_time


def read_from_dynamo(*, params: dict, Table) -> dict:
    try:
        return Table.scan(**params)
    except ClientError as boto3ClientError:
        raise boto3ClientError


"""
@:param lap name and the year prefix (if year = 2019 -> prefix = 20)
@:return the lap start date in the right syntax (for mysql) by parsing
the lap name.
"""


def calc_lap_start_date(lap_name, yearPrefix) -> str:
    str_end = len(lap_name) - 3
    seconds = lap_name[str_end - 2:str_end]
    str_end = str_end - 2
    minutes = lap_name[str_end - 2:str_end]
    str_end = str_end - 2
    hour = lap_name[str_end - 2:str_end]
    str_end = str_end - 2
    year = lap_name[str_end - 2:str_end]
    str_end = str_end - 2
    day = lap_name[str_end - 2:str_end]
    str_end = str_end - 2
    month = lap_name[str_end - 2:str_end]

    lap_start_date = f"{yearPrefix}{year}-{month}-{day} {hour}:{minutes}:{seconds}"
    return str(lap_start_date)


def calculate_acc_comb(lat_sum, long_sum, num_of_items):
    acc_comb = 0
    if num_of_items != 0:
        lat_avg = lat_sum / num_of_items
        long_avg = long_sum / num_of_items
        acc_comb = math.sqrt(math.pow(lat_avg, 2) + math.pow(long_avg, 2))
    return acc_comb


def calculate_kpi(lap, config):
    lapId = lap.getLapName()
    rows: [] = lap.getLapQuads()
    num_of_points: int = environ('kpi_num_of_points', int)
    tasks: list = []
    loop = asyncio.get_event_loop()

    limit, page = environ('runDataRetrieveLimit', int), environ('runDataPaging', int)

    _payload = {'lapName': lap.getLapName(), 'page': page, 'limit': limit}

    async def invokeLambdaKpi(lambdaName: str, payload={}) -> {}:
        res = lambdaClient.invoke(FunctionName=lambdaName, InvocationType='RequestResponse',
                                  Payload=json.dumps({'lambdaName': lambdaName, 'payload': payload}))
        return json.loads(res['Payload'].read())

    for field, task in config.fieldToCalculateKpi.items():
        tasks.append(loop.create_task(
            invokeLambdaKpi(lambdaName=task['lambda'], payload=_payload)))  # , payload=lap.getLapQuads())))

    done, _ = loop.run_until_complete(asyncio.wait(tasks))
    _kpi_dict: {} = {}
    for fut in done:
        try:
            res = json.loads(fut.result()['body'])['value']
            _kpi_dict = {**_kpi_dict, **res}
            print("return value is {}".format(res['value']))
        except KeyError as ke:
            print(f"key error in coroutine result is: \n {fut.result()}")
    loop.close()
    return _kpi_dict
