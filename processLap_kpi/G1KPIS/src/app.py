import asyncio
import json
import os
import traceback
from asyncio import Task, Future
import functools
import numpy as np
# import bottleneck as bn


# import requests
from api_wrapper import ApiWrapper
from griiip_exeptions import *

#
KPI_DICT: dict = {
    'throttle': [],
    'longAcc': [],
    'latAcc': [],
    'brakePresF': [],
    'lapTime': []
}


# Todo add lumigo tracing
def lambda_handler(event, context):
    quads: [] = retrieveLapRunDataLapQuads(payload=event['payload'])
    kpi_name: str = event['lambdaName']
    NOP = int(os.environ['num_of_points'])
    throttle, longAcc, latAcc, brakePresF, lapTime = [], [], [], [], []
    for q in quads:
        throttle.append(float(q['throttle']))
        longAcc.append(float(q['longAcc']))
        latAcc.append(float(q['latAcc']))
        brakePresF.append(float(q['brakePresF']))
        lapTime.append(float(q['lapTime']))

    KPI_DICT['throttle'], KPI_DICT['longAcc'], KPI_DICT['latAcc'], KPI_DICT['brakePresF'], \
    KPI_DICT['lapTime'] = throttle, longAcc, latAcc, brakePresF, lapTime

    tasks: list = []
    loop = asyncio.get_event_loop()
    async_res: dict = {}

    # asynchronous calculate rolling mean
    for kpi in KPI_DICT.keys():
        _t: Future = asyncio.ensure_future(async_rolling_mean(l=KPI_DICT[kpi], N=NOP))  # create the async task to
        # calculate rolling mean
        name: str = f"filtered_{kpi}"  # give th task name filtered_{"kpi_name}
        _t.add_done_callback(functools.partial(asyncCallback, kpiName=name, _dict=async_res))  # add callback function
        # to be call when task is done
        tasks.append(_t)  # add task to task list

    loop.run_until_complete(asyncio.wait(tasks))
    #loop.close()

    kpiDict: {} = calculate(filteredListsDict=async_res)  # calculate KPI
    return {
        "statusCode": 200,
        "body": json.dumps({
            "kpi": kpi_name,
            "value": kpiDict,
            # "location": ip.text.replace("\n", "")
        }),
    }


def calculate(filteredListsDict: dict, lapName='test') -> {}:
    throttle = KPI_DICT['throttle']
    diff = []

    thr_smooth = 0
    thr_eff = 0
    thr_count = 0

    brk_eff = 0
    brk_count = 0

    brk_agr = 0
    brk_agr_count = 0

    brk_stb = 0
    brk_stb_count = 0

    trl_acc = 0
    trl_acc_count = 0
    # assign all async results to arrays
    filtered_throttle = filteredListsDict['filtered_throttle']
    filtered_longAcc = filteredListsDict['filtered_longAcc']
    filtered_latAcc = filteredListsDict['filtered_latAcc']
    filtered_brakePresF = filteredListsDict['filtered_brakePresF']
    filtered_lapTime = filteredListsDict['filtered_lapTime']

    brake_diff = vector_diff(vector=filtered_brakePresF, time_vector=filtered_lapTime)  # create braking diff array
    # todo: create a function for this part
    for index, i in enumerate(filtered_throttle):

        # Throttle smoothness
        throttle_diff = float(abs(throttle[index] - i))
        diff.append(throttle_diff)
        thr_smooth += throttle_diff

        # Throttle efficiency
        if filtered_longAcc[index] > 0 and throttle[index] > 5:
            thr_eff += filtered_longAcc[index] / throttle[index]
            thr_count += 1

        # Braking efficiency
        elif filtered_longAcc[index] < 0 and filtered_brakePresF[index] > 1:
            brk_eff += -1 * filtered_longAcc[index]
            brk_count += 1

        # Braking aggressiveness
        if brake_diff[index] > 0:
            brk_agr += brake_diff[index]
            brk_agr_count += 1

        # Trail breaking
        if filtered_brakePresF[index] > 0 and abs(filtered_latAcc[index]) > 0.1:
            brk_stb += filtered_brakePresF[index] * abs(filtered_latAcc[index])
            brk_stb_count += 1

        # Trail Acceleration
        if throttle[index] > 5 and abs(filtered_latAcc[index]) > 0.1:
            trl_acc += throttle[index] * abs(filtered_latAcc[index])
            trl_acc_count += 1

    if thr_count == 0 or len(diff) == 0 or brk_count == 0 \
            or brk_agr_count == 0 or brk_stb_count == 0 or trl_acc_count == 0:
        print("""WARNING:\tNOT ENOUGH DATA TO CALCULATE LAP: {} KPI.""".format(lapName))
        return

    return {"throttleSmoothness": thr_smooth / len(diff),
            "throttleEfficiency": thr_eff / thr_count,
            "brakeEfficiency": brk_eff / brk_count,
            "brakepresAggression": brk_agr / brk_agr_count,
            "trailBraking": brk_stb / brk_stb_count,
            "trailAcceleration": trl_acc / trl_acc_count
            }


"""
@function asyncCallback callback after the async task that calculate the filtered list
assign result to list in the result_dict
@:param @future the ended task
        @kpiName the name of the kpi that bean calculated 
        @_dict dict that hold all the results by they kpi name  
"""


def asyncCallback(future, kpiName: str, _dict: dict):
    _dict[kpiName] = future.result()


"""
@retrieveLapRunData : function that get all the runData of the lap By lapId
from 'driverlapsrundata' Table in RDS 
@:param lapId the lapId to get its all data from driverLapsRunData table
@:return array of type RunDataRow each object in the array is one record 
of the lapRunData
"""


def retrieveLapRunDataLapQuads(payload: dict) -> []:
    # call API to get runData
    runData: dict = ApiWrapper.get("/rundata/", params=payload).json()['data']

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
    num_dist_glitch: int = removed_first_bad_distance_rows()
    if num_dist_glitch > 0:
        print(f"FOUND {num_dist_glitch} BAD ROWS FOR LAP {payload['lapName']}"
              f"\nLAP FIRST ROWS DISTANCE IS BIGGER THEN THE NEXT ROWS")

    return runData[num_dist_glitch:]  # remove the rows with the distance glitches in the


"""
@:function rolling_mean
calculates the moving average on the points given in the list.
"""


async def async_rolling_mean(*, l, N) -> []:
    def runningMean():
        y = np.zeros((len(l),))

        for ctr in range(len(l)):
            y[ctr] = np.sum(l[ctr:(ctr + N)])  # get the moving sum according to the rolling windows

        return y / N  # davide each of sum's in the window size

    try:
        return list(runningMean())  # np.convolve(l, np.ones((N,)) / N, mode='valid'))  #
        # bn.move_mean(l, window=N,
        # min_count=1))
    except Exception as e:
        print("ERROR IN rolling_mean\n {}".format(e))
        print(traceback.format_exc())
        return []


"""
@:function vector_diff
calculates a new vector (list) by calculating the difference between two following
coordinates divided by the time between those two points
"""


def vector_diff(*, vector, time_vector):
    diff_vec = np.diff(vector) / np.diff(time_vector)
    return list(np.concatenate(([0], diff_vec)))
