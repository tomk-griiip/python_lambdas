from api_wrapper import ApiWrapper
from beans import *
from griiip_exeptions import ApiException, RunDataException, DriverLapsException, TracksException
from config import config as conf
from classifiers import ruleBaseClassifier
from interfaces import Iclassifier
import traceback
from lambda_utils import *

import sys

dynamoDb = boto3.resource('dynamodb')
laps_from_dynamo_table = os.environ['ddb_lap_table']

"""
lambda_handler 
@:returns ensure{
status code : 200 o.k
              501 griiip custom error 
              502 type error
              500 general error
message: the response message
"""


def lambda_handler(event, context):
    ERROR_PREFIX: str = "ERROR::"
    res_message: str = "consumerOk"
    trace: str = ""
    statusCode: int = 200
    ddb_lap_table, ddb_lap_table_key = environ('ddb_lap_table'), environ('ddb_lap_table_key')

    try:
        for record in event['Records']:
            recordBody: dict = record['body']
            handle_lap(record=recordBody)

    except (RunDataException, DriverLapsException, TracksException, ApiException)as griiipException:
        trace = traceback.format_stack()
        res_message = griiipException
        statusCode = 501  # griiip custom exception status code
        print(f"{ERROR_PREFIX} {type(griiipException)} error happened info : "
              f"{griiipException} \n {trace}")

    except TypeError as te:
        res_message = te
        statusCode = 502
        trace = traceback.format_stack()
        print(f"{ERROR_PREFIX} type error happened info : {te} \n {trace}")

    except Exception as e:
        trace = traceback.format_stack()
        res_message = e
        statusCode = 500  # general exception status code
        print(f"{ERROR_PREFIX} error {e} \n trace :: -> {trace}")

    finally:
        return {
            "statusCode": statusCode,
            "body": json.dumps({
                "message": f"{res_message}",
                "trace": f"{trace}",
            }),
        }


def handle_lap(record: dict):
    lapId = record["lapId"]
    lapQuadsArr: [] = retrieveLapRunDataLapQuads(lapId)
    # create object that represent full lap
    for key, value in conf.fieldsFromSqsMessage.items():
        conf.driverLapFieldDict[key] = record[value]
    try:
        # create Lap object that represent the current lap that being process
        lap = Lap(lap_quads=lapQuadsArr, funcToField=conf.driverLapFuncToCalcField, **conf.driverLapFieldDict)
        lap.set_track_length(ApiWrapper)  # set the length of the track that the lap is on
        lapClass: str = classifyLap(lap=lap, classifier=ruleBaseClassifier)  # classify the lap
        lap.set_classification(classification=lapClass)  # set the class to the lap

        if lapClass in conf.classify_that_calc_kpi_list:  # if the classification need kpi calculation
            kpi: {} = calculate_kpi(lap, conf)  # calculate kpi
            lap.add_columns_to_columns_to_update(kpi)  # add the result to columns to update
        pass

    except (RunDataException, DriverLapsException, TracksException) as griiip_e:
        print(f"LAP: {lapId} IS MISSING DATA IN MYSQL, Exception raised is: {griiip_e}")
        raise griiip_e

    except Exception as e:
        raise e

    pass


"""
@retrieveLapRunData : function that get all the runData of the lap By lapId
from 'driverlapsrundata' Table in RDS 
@:param lapId the lapId to get its all data from driverLapsRunData table
@:return array of type RunDataRow each object in the array is one record 
of the lapRunData
"""


def retrieveLapRunDataLapQuads(lapId: str) -> []:
    # config the request for API to get the run data
    limit, page = int(os.environ['runDataRetrieveLimit']), \
                  int(os.environ['runDataPaging'])

    payload = {'lapName': lapId, 'page': page, 'limit': limit}
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
    num_dist_glit: int = removed_first_bad_distance_rows()
    if num_dist_glit > 0:
        print(f"FOUND {num_dist_glit} BAD ROWS FOR LAP {lapId}"
              f"\nLAP FIRST ROWS DISTANCE IS BIGGER THEN THE NEXT ROWS")

    runData = runData[num_dist_glit:]  # remove the rows with the distance glitches in the
    # beginning

    # create array of RunDataRow bean object
    return [RunDataRow(**runData[i]) for i in range(len(runData))]


# wrapper function to classify lap
def classifyLap(lap: Lap, classifier: Iclassifier = Iclassifier()) -> str:
    return classifier.classify(lap=lap, api=ApiWrapper)
