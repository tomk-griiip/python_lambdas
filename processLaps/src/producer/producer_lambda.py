import json

from ..handlers import LapBean
from ..lambda_utils import *
from ..griiip_exeptions import *
from ..griiip_const import net
import boto3
import os
from ..db_wrapper import DbPyMySQL, DynamoDb
# as ddb, api as api_db
import traceback
from ..sql_pool_connection import ConnectionPool
from . import logger
"""
create sql pool connection
"""
rdsConfig = {'host': os.environ["my_sql_host"],
             'user': os.environ["my_sql_user"],
             'password': os.environ["my_sql_pass"],
             'database': os.environ["my_sql_db"],
             'autocommit': False
             }

mySqlPool = ConnectionPool(size=int(os.environ["rds_connection_pull_size"]), name='pool1', **rdsConfig)
sqs = boto3.client('sqs')
queueUrl = os.environ['responseQueue']
PROCESS_NAME = "laps_producer"
DDB_CACHE_TABLE = os.environ['cache_ddb_table_name']


def put_previous_lap_to_sqs(lap: LapBean) -> bool:
    """
    put_lap_to_sqs put the lap id that the consumer lambda need to process in sqs
    :param lap: lap id to pass to the next lambda
    :return: true for success false for failre
    """
    try:
        lap_prefix: str = lap.lapId[:-3]
        lap_number: int = int(lap.lapId[-3:]) - 1
        previous_lap_id = f"{lap_prefix}{int_to_tree_digit_string(lap_number)}"
        sqsMessage = "{" + f'"lapId":"{previous_lap_id}","trackId":"{lap.lap.trackId}",' \
                           f'"careId":"{lap.lap.carId}","userId":"{lap.lap.userId}"' + "} "

        res = sqs.send_message(
            QueueUrl=queueUrl,
            MessageBody=sqsMessage
        )

        if res['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error(f"error in sqs function sqs message : {sqsMessage}")
            raise ClientError

    except Exception as e:
        logger.error(f"sqs error : {e}")
        return False
    return True
    logger.info(f"sqs response {res}")


class LambdaLogic:
    """
    class that handle all the lambda logic
    """

    def __init__(self, db: DbPyMySQL, ddb: DynamoDb):
        self.db = db
        self.ddb = ddb

    def handle_record(self, record: dict):
        """
        process the lap and insert it with the lap number to cache_table
        :param record: record the new lap to process (without lap number )

        """
        lap = LapBean(record=record)
        try:  # insert lap id to cache table in order to follow the lap number
            # even if the process failed in the way (not inserted to sql / sqs )
            # the lap count should still need to keep going
            ddb = DynamoDb()
            is_success, lap, batch = self.insert_lap_to_dynamo_db_cache_table(lap=lap)
            if not is_success:
                logger.warning(f"{PROCESS_NAME}: error at insert_lap_to_dynamo_db_cache_table")
                return
            # try to insert lap to mysql (but do n ot commit )
            if not self.insert_lap_to_mysql_no_commit(lap=lap):
                logger.warning(f"{PROCESS_NAME}: error at insert_lap_to_mysql_no_commit")
                return
            # check if lap number is > 0 if true put the previous lap to process
            # (because if this lap was insert to queue its mean previous lap is finish)
            not_lap_zero: bool = True if int(lap.lapId[-3:]) > 0 else False
            # if lap number is > 0 then insert the previous  lap to sqs for process
            # and
            # lap inserted to mysql insert the lap to sqs queue to keep calculate this lap
            if not_lap_zero and not put_previous_lap_to_sqs(lap=lap):
                logger.warning(f"{PROCESS_NAME}: error at put_lap_to_sqs")
                return  # TODO need to write rollback logic in failed scenario
            self.mysql_commit()  # commit transaction to mysql

        except DynamoDbBadStatusCode as dbs:
            logger.error(f"{PROCESS_NAME} error : {dbs}")

        except ClientError as cerr:
            logger.error(traceback.format_stack())
            logger.error(f"{PROCESS_NAME} error: {cerr}")

        except Exception as e:
            logger.error(traceback.format_stack())
            logger.error(f"{PROCESS_NAME} error: {e}")
        finally:
            return lap.lapId

    def insert_lap_to_mysql_no_commit(self, lap: LapBean) -> bool:
        """
        insert new lap to mySql db
        :param db:
        :param lap: lap to insert
        :return: True for success false for failed
        """
        lap_time = format_seconds_to_hhmmss(0.0)
        insert: str = f"insert into `driverlaps`(`lapName`, `TrackId`, `CarId`, `UserId`, `lapStartDate`, " \
                      f"`lapTime`) " \
                      f"VALUES('{lap.lapId}', '{lap.lap.trackId}', '{lap.lap.carId}', " \
                      f"'{lap.lap.userId}', '{lap.lap.lapStartTime}', '{lap_time}')"
        try:
            self.db.put(sql_cmd=insert, not_commit=True)
            logger.info("""ADDED LAP: {} INTO DRIVERLAPS TABLE""".format(lap.lapId))
            return True
        except Exception as e:
            logger.error(f"error when insert lap {lap.lapId} \n error: {e}")
            return False

    def insert_lap_to_dynamo_db_cache_table(self, lap: LapBean) -> bool:
        """
        insert new lap to cache table in dynamoDb for saving the persistent of the process laps
        :param ddb:
        :param lap: lap to cache
        @:raise DynamoDbBadStatusCode
        :return: true for success false for failre
        """

        def query_last_lap_number() -> str:
            """

            :param prefix_lap: the lap id without the lap number that is an know yet
            :return: the last lap number + 1 (the current lap) as string
            """
            items = self.ddb.get(tableName=DDB_CACHE_TABLE,
                                 key=os.environ['cache_ddb_table_key'],
                                 eq=lap_prefix, ScanIndexForward=False, Limit=1)
            return 0 if len(items) == 0 else max([int(v['lap_number']) for i, v in enumerate(items)]) + 1

        lap_prefix: str = lap.lapId[:-3]
        lap_number: str = int_to_tree_digit_string(query_last_lap_number())
        lap.lapId = f"{lap_prefix}{lap_number}"
        try:
            batch = self.ddb.put(tableName=DDB_CACHE_TABLE, items=[
                {
                    'prefix_lap_id': lap_prefix,
                    'lap_number': lap_number,
                    'lap_id': lap.lapId
                }
            ])
        except Exception as e:
            logger.error(f"exeption in insert lap to dynamo db lap number {lap.lapId}\n exception: {e}")
            raise DynamoDbBadStatusCode(statusCode=500)
            return False, lap

        return True, lap, batch

    def mysql_commit(self) -> bool:
        """
        commit the last changes on the db
        :param conn: mysql connaction
        :return: true for success false for failre
        """
        try:
            self.db.commit()
            return True
        except Exception as e:
            raise e
            return False


def lambda_handler(event, context):
    laps: list = []
    # iterate over all the records in the message
    db = DbPyMySQL(mySqlPool)
    ddb = DynamoDb()
    try:
        _record = json.loads(event['body'])
        __lapId = LambdaLogic(db=db, ddb=ddb).handle_record(record=_record)
        laps.append(__lapId)

    except Exception as e:
        logger.error(f"exception laps producer : {e} \n {traceback.format_exc()}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"laps {','.join(laps)} was moved to process ",
        }),
    }
