from beans import LapBean
from lambda_utils import *
from griiip_exeptions import *
from api_wrapper import ApiWrapper
import boto3
from boto3.dynamodb.conditions import Key
import pymysql
import os


# init dynamoDb table
dynamoDb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')
cache_table = dynamoDb.Table(os.environ['cache_ddb_table_name'])  # the dynamo table that hold the
queueUrl = os.environ['responseQueue']
mySqlConn = pymysql.connect(host=os.environ['my_sql_host'],
                            user=os.environ['my_sql_user'],
                            password=os.environ['my_sql_pass'],
                            db=os.environ['my_sql_db'])
cursor = mySqlConn.cursor()
PROCESS_NAME = "laps_producer"

"""
@:parameter prefix_lap the lap id without the lap number that is an know yet
@ return the last lap number + 1 (the current lap) as string 
"""


def query_last_lap_number(prefix_lap: str) -> str:
    kce = Key(os.environ['cache_ddb_table_key']).eq(prefix_lap)  # & Key('lap_number').between(0, 999)
    ddb_res = cache_table.query(
        KeyConditionExpression=kce, ScanIndexForward=False, Limit=1

    )
    items = ddb_res['Items']
    return 0 if len(items) == 0 else max([int(v['lap_number']) for i, v in enumerate(items)]) + 1


def lambda_handler(event, context):
    laps: list = []
    # iterate over all the records in the message
    for record in event['Records']:
        try:
            __lapId = handle_record(record['body'])
            laps.append(__lapId)

        except Exception as e:
            print(f"exception laps producer : {e}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"laps {','.join(laps)} was moved to process ",
        }),
    }


"""
@:param record the new lap to process (without lap number )
process the lap and insert it with the lap number to cache_table
"""


def handle_record(record: dict):
    lap = LapBean(record=record)
    try:  # insert lap id to cache table in order to follow the lap number
        # even if the process failed in the way (not inserted to sql / sqs )
        # the lap count should still need to keep going
        is_success, lap = insert_lap_to_dynamo_db_cache_table(lap=lap)
        if not is_success:
            print(f"{PROCESS_NAME}: error at insert_lap_to_dynamo_db_cache_table")
            return
        # try to insert lap to mysql (but do n ot commit )
        if not insert_lap_to_mysql_no_commit(lap=lap):
            print(f"{PROCESS_NAME}: error at insert_lap_to_mysql_no_commit")
            return
        # check if lap number is > 0 if true put the previous lap to process
        # (because if this lap was insert to queue its mean previous lap is finish)
        not_lap_zero: bool = True if int(lap.lapId[-3:]) > 0 else False
        # if lap number is > 0 then insert the previous  lap to sqs for process
        # and
        # lap inserted to mysql insert the lap to sqs queue to keep calculate this lap
        if not_lap_zero and not put_previous_lap_to_sqs(lap=lap):
            print(f"{PROCESS_NAME}: error at put_lap_to_sqs")
            return  # TODO need to write rollback logic in failed scenario
        mysql_commit(mySqlConn)  # commit transaction to mysql

    except DynamoDbBadStatusCode as dbs:
        print(f"{PROCESS_NAME} error : {dbs}")

    except Exception as e:
        print(f"{PROCESS_NAME} error: {e}")
    finally:
        return lap.lapId


"""
insert new lap to mySql db
@:param lap to insert
@:return True for success false for failed 
"""


def insert_lap_to_mysql_no_commit(lap: LapBean) -> bool:
    lap_time = format_seconds_to_hhmmss(0.0)
    insert: str = f"insert into `driverlaps`(`lapName`, `TrackId`, `CarId`, `UserId`, `lapStartDate`, " \
                  f"`lapTime`) " \
                  f"VALUES(%s, %s, %s, %s, %s, %s)"
    try:
        cursor.execute(insert,
                       (lap.lapId, lap.lap.trackId, lap.lap.carId, lap.lap.userId, lap.lap.lapStartTime, lap_time))

        print("""ADDED LAP: {} INTO DRIVERLAPS TABLE""".format(lap.lapId))
        return True
    except Exception as e:
        print(f"error when insert lap {lap.lapId} \n error: {e}")
        return False


"""
insert new lap to cache table in dynamoDb for saving the persistent
of the process laps 
@:param lap to cache
@:raise DynamoDbBadStatusCode 
"""


def insert_lap_to_dynamo_db_cache_table(lap: LapBean) -> bool:
    lap_prefix: str = lap.lapId[:-3]
    lap_number: str = int_to_tree_digit_string(query_last_lap_number(lap_prefix))
    lap.lapId = f"{lap_prefix}{lap_number}"
    res_status_code = cache_table.put_item(
        Item={
            'prefix_lap_id': lap_prefix,
            'lap_number': lap_number,
            'lap_id': lap.lapId
        }
    )['ResponseMetadata']['HTTPStatusCode']
    if res_status_code is not 200:
        raise DynamoDbBadStatusCode(statusCode=res_status_code)
        return False, lap
    return True, lap


"""
@:function put_lap_to_sqs put the lap id that the consumer lambda 
need to process in sqs
@:param lap id to pass to the next lambda
"""


def put_previous_lap_to_sqs(lap: LapBean) -> bool:
    try:
        lap_prefix: str = lap.lapId[:-3]
        lap_number: int = int(lap.lapId[-3:]) - 1
        previous_lap_id = f"{lap_prefix}{int_to_tree_digit_string(lap_number)}"
        res = sqs.send_message(
            QueueUrl=queueUrl,
            MessageBody=(json.dumps({"lapId": previous_lap_id}))
        )
    except Exception as e:
        print(f"sqs error : {e}")
        return False
    return True
    print(f"sqs response {res}")


"""
@:param mysql connaction
commit the last changes on the db 
"""


def mysql_commit(conn) -> bool:
    try:
        conn.commit()
        return True
    except Exception as e:
        raise e
        return False


"""
use the API get awy to insert first time lap to mysql 
@:param LapBean with full lapId lapTime = 0.0
carId UserId and lapStartTime
"""


def add_new_lap_mysql_api(lap: LapBean) -> bool:
    lap_time = format_seconds_to_hhmmss(0.0)
    try:
        ApiWrapper.put("/driverlaps/", json={'lapName': lap.lapId,
                                             'lapStartDate': lap.lap.lapStartTime,
                                             'lapTime': lap_time,
                                             'UserId': lap.lap.carId, 'TrackId': lap.lap.userId,
                                             'CarId': lap.lap.carId})
        return True
    except Exception as e:
        print(f"error at add_new_lap_mysql_api :  {e}")
        return False
