import json
from db_wrapper import sql as db


def lambda_handler(event, context):
    if 'body' not in event or 'sessionId' not in event['body']:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"request need to have body and sessionId ",
            }),
        }
    sessionId = event['body']['sessionId']
    query: str = "select tracksessions.timeStart,tracksessions.timeEnd, trackevents.TrackId from tracksessions " \
                 "inner join trackevents on tracksessions.TrackEventId = trackevents.id " \
                 f"where tracksessions.id={sessionId}"
    res = db.get(sql_cmd=query)

    print(f'hiii: {res}')
