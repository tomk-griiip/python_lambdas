from src.db_wrapper import DbApiWrapper
from ..handlers import *
from ..griiip_exeptions import *
from ..config import config as conf
from ..classifiers import ruleBaseClassifier
from ..classifiers import IClassifier
import traceback
from multiprocessing import Process, Pipe

from ..interfaces import IDataBaseClient
from ..lambda_utils import *
from ..griiip_const import net, classifications, errorMessages
from ..db_wrapper import DbApiWrapper

db = DbApiWrapper(api_address=environ('griiip_api_url'), api_key=environ('griiip_api_key'))
laps_from_dynamo_table = os.environ['ddb_lap_table']
t = None

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
    ERROR_PREFIX: str = "ERROR::"
    res_message: str = "consumerOk laps that was clculate : "
    trace: str = ""
    statusCode: int = 200
    ddb_lap_table, ddb_lap_table_key = environ('ddb_lap_table'), environ('ddb_lap_table_key')

    try:
        """ 
        lambda withe multi process rafrens from https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/
        """
        # lap ids that was process successful
        successLaps: [] = []

        # error from laps that failed
        failedLaps: [] = []

        # create a list to keep all processes
        processes = []

        # create a list to keep connections
        parent_connections = []
        for record in event['Records']:
            payload: dict = record['body']
            for _record in payload:
                parent_conn, child_conn = Pipe()
                parent_connections.append(parent_conn)
                p = Process(target=handle_lap, args=(_record, child_conn))
                processes.append(p)

        # start all processes
        for process in processes:
            process.start()

        # make sure that all processes have finished
        for process in processes:
            process.join()

        # get responses from all process
        for parent_connection in parent_connections:
            processRes = parent_connection.recv()
            if not processRes[0]:
                failedLaps.append(processRes[1])
            successLaps.append(processRes[1])

        res_message = f"{res_message}\n {successLaps}"

        # if there were errors in one of the laps
        if len(failedLaps) > 0:
            res_message = f"{res_message}\n errors:: \n {failedLaps}"

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


def handle_lap(record: dict, conn):
    """

    :param record: the lap
    :param conn: process pip connection
    :return: array [0] = false => exception raised, true => return the lapId
    """
    lapId = record["lapId"]
    # config the request for API to get the run data
    limit, page = int(os.environ['runDataRetrieveLimit']), int(os.environ['runDataPaging'])
    runData: [] = RunData.getRunData(db=db, lapName=lapId, limit=limit, page=page)
    error = None

    # create object that represent full lap
    for key, value in conf.fieldsFromSqsMessage.items():
        conf.driverLapFieldDict[key] = record[value]

    # create Lap object that represent the current lap that being process
    lap = Lap(runData=runData, funcToField=conf.driverLapFuncToCalcField, db=db, **conf.driverLapFieldDict)

    try:
        lap.set_track_length()  # DbApiWrapper# set the length of the track that the lap is on
        lapClass: str = classifyLap(lap=lap, classifier=ruleBaseClassifier)  # classify the lap
        lap.set_classification(classification=lapClass)  # set the class to the lap

        if lapClass in conf.classify_that_calc_kpi_list:  # if the classification need kpi calculation
            with AsyncLoopManager() as loop:
                kpi: {} = calculate_kpi(loop=loop, lapId=lap.getLapName(), config=conf)  # calculate kpi
            lap.setColumnsToUpdate(kpi)  # add the result to columns to update

    except KpiLambdaError as kpiError:
        error = f"{lapId} {errorMessages.MYSQL_MISSING_DATA} {kpiError}"
        print(f"{error} \n {traceback.format_stack()}")

    except (RunDataException, DriverLapsException, TracksException) as griiip_e:
        error = f"{lapId} {errorMessages.MYSQL_MISSING_DATA} \n {griiip_e}"
        print(f"{error} \n {traceback.format_stack()}")

    except Exception as e:
        error = f"Exception at lapId {lapId}\n {e}"
        print(f"{error} \n {traceback.format_stack()}")

    finally:
        columnToUpdate: {} = lap.getColumnToUpdate()
        if len(columnToUpdate.keys()) > 0:  # if there is no items to update return
            lap.updateDriverLap()

        if error is not None:
            """
            if error or exception raise error is not None 
            and need to return the error to parent process 
            with inductor [0] = false that indicate that lapId was failed to be process 
            """
            conn.send([False, error])

        # if lap process success return to parent process the lapId with [0] = True
        conn.send([True, lapId])
        conn.close()


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

    if not isinstance(classifier, IClassifier):
        raise InterfaceImplementationException('IClassifier')

    return classifier.classify(lap=lap, api=DbApiWrapper)
