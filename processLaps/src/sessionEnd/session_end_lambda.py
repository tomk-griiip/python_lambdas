import json
import os
import boto3
import traceback
from botocore.exceptions import ClientError
from ..db_wrapper import DbPyMySQL  # sql as db
from ..sql_pool_connection import ConnectionPool

"""
create sql pool c
"""
rdsConfig = {'host': os.environ["my_sql_host"],
             'user': os.environ["my_sql_user"],
             'password': os.environ["my_sql_pass"],
             'database': os.environ["my_sql_db"],
             'autocommit': False
             }

mySqlPool = ConnectionPool(size=int(os.environ["rds_connection_pull_size"]), name='pool1', **rdsConfig)

QUEUE_URL = os.environ['QUEUE_URL']
sqs = boto3.client('sqs')


def lambda_handler(event, context):
    statusCode = 200
    message = "session end all lasts lap moved to process"
    db = DbPyMySQL(mySqlPool=mySqlPool)
    try:
        if 'body' not in event or 'sessionId' not in event['body']:
            statusCode = 500
            message = "request need to have body and sessionId "
            return

        sessionId = event['body']['sessionId']
        query: str = "select tracksessions.timeStart,tracksessions.timeEnd, trackevents.TrackId from tracksessions " \
                     "inner join trackevents on tracksessions.TrackEventId = trackevents.id " \
                     f"where tracksessions.id={sessionId}"
        session = db.get(sql_cmd=query, use_dict_cursor=True, first=True)
        if not session:
            statusCode = 400
            message = f"session not found {sessionId}"
            return

        sessionInfo = {
            "timeStart": session['timeStart'],
            "timeEnd": session['timeEnd'],
            "TrackId": session['TrackId']
        }

        message, statusCode = handleSession(sessionId=sessionId, **sessionInfo, db=db)

    except Exception as e:
        statusCode = 500
        message = e

    finally:
        return {
            "statusCode": statusCode,
            "body": json.dumps({
                "message": f"{message}",
            }),
        }


def handleSession(sessionId: str, **kwargs) -> tuple:
    """

    :param sessionId:
    :param kwargs:
    :return: tuple of return message and statusCode
    """
    timeStart, timeEnd, trackId = kwargs['timeStart'], kwargs['timeEnd'], kwargs['TrackId']
    """
    query all laps from 'driverlaps' table order by the lap number (should be the last lap )
    group by care id the careId from the lapNme
    """

    db = kwargs['db']
    query: str = f"select laps.* from(" \
                 f"select {os.environ['driverLapFiledsToQuery']} from {os.environ['driverLapsTable']} " \
                 f"where lapStartDate BETWEEN '{timeStart}' and '{timeEnd}' and TrackId = {trackId} " \
                 f"order by right(lapName, 3) desc " \
                 f") as laps group by LEFT(laps.lapName, 3)"
    try:
        finishedLaps = db.get(sql_cmd=query, use_dict_cursor=True)
        lapsToSendToConsumer: [] = []
        """
        extract the lap info and insert it to list in order to send to 
        consumer lambda for processing 
        """
        for lap in finishedLaps:
            lapsToSendToConsumer.append({
                "lapId": lap['lapName'],
                "trackId": lap['TrackId'],
                "carId": lap['CarId'],
                "userId": lap['UserId']
            })

        """
        send the laps that need to be process to sqs
        """
        message: str = json.dumps(lapsToSendToConsumer)
        sqs_res = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=message
        )

        if sqs_res['ResponseMetadata']['HTTPStatusCode'] != 200:
            """
            if the return status cose from sqs is not 2000
            raise boto3 ClientError exception 
            """
            raise ClientError

        return (f"{sqs_res}\n session {sessionId} finished. "
                f"all laps {lapsToSendToConsumer} was pass to consumer", 200)

    except ClientError as cle:
        return f"error in closing session {sessionId}\n raise {cle}", 500

    except Exception as e:
        print(f" exception raise in closing session lambda \n trace : "
              f"{traceback.format_stack()}")
        raise e
