from api_wrapper import ApiWrapper
from handlers import *
from griiip_exeptions import *
from config import config as conf
from classifiers import ruleBaseClassifier
from classifiers import IClassifier
import traceback

from interfaces import IDataBaseClient
from lambda_utils import *
from griiip_const import net, classifications, errorMessages
from db_wrapper import db_api as db

dynamoDb = boto3.resource('dynamodb')
laps_from_dynamo_table = os.environ['ddb_lap_table']


def lambda_handler(event, context):
    """

    Parameters
    ----------
    event
    context

    Returns
    -------
    {
        status code : 200 o.k
                      501 griiip custom error
                      502 type error
                      500 general error
        message: the response message
    }
    """
    t = environ('test')
    print(t)
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
    # config the request for API to get the run data
    limit, page = int(os.environ['runDataRetrieveLimit']), int(os.environ['runDataPaging'])
    runData: [] = db.getRunData(query=net.RUNDATA_URL, lapName=lapId, limit=limit, page=page)

    # create object that represent full lap
    for key, value in conf.fieldsFromSqsMessage.items():
        conf.driverLapFieldDict[key] = record[value]

    # create Lap object that represent the current lap that being process
    lap = Lap(runData=runData, funcToField=conf.driverLapFuncToCalcField, **conf.driverLapFieldDict)

    try:
        lap.set_track_length(ApiWrapper)  # set the length of the track that the lap is on
        lapClass: str = classifyLap(lap=lap, classifier=ruleBaseClassifier)  # classify the lap
        lap.set_classification(classification=lapClass)  # set the class to the lap

        if lapClass in conf.classify_that_calc_kpi_list:  # if the classification need kpi calculation
            with AsyncLoopManager() as loop:
                kpi: {} = calculate_kpi(loop=loop, lapId=lap.getLapName(), config=conf)  # calculate kpi
            lap.setColumnsToUpdate(kpi)  # add the result to columns to update

    except KpiLambdaError as kpiError:
        print(f"{lapId} {errorMessages.MYSQL_MISSING_DATA} {kpiError}")
        raise KpiLambdaError

    except (RunDataException, DriverLapsException, TracksException) as griiip_e:
        print(f"{lapId} {errorMessages.MYSQL_MISSING_DATA} {griiip_e}")
        raise griiip_e

    except Exception as e:
        raise e

    finally:
        columnToUpdate: {} = lap.getColumnToUpdate()
        if len(columnToUpdate.keys()) > 0:  # if there is no items to update return
            db.updateDriverLap(columns_to_update=lap.getColumnToUpdate(), lap_name=lap.getLapName())


def classifyLap(lap: Lap, classifier: IClassifier) -> str:
    """
    wrapper function to classify lap
    Parameters
    ----------
    lap
    a lap object to classify
    classifier
    a class that implement I_classifier
    Returns
    -------
    classification
    """

    if not issubclass(classifier, IClassifier):
        raise InterfaceImplementationException('IClassifier')

    return classifier.classify(lap=lap, api=ApiWrapper)
