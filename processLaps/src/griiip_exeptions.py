class RunDataException(Exception):
    def __init__(self, lap_name):
        Exception.__init__(self, "Couldn't find lap: {} in RUN-DATA table".format(lap_name))


class DriverLapsException(Exception):
    def __init__(self, lap_name):
        Exception.__init__(self, "Couldn't find lap: {} in DRIVER-LAPS table".format(lap_name))


class TracksException(Exception):
    def __init__(self, track):
        Exception.__init__(self, "Couldn't find track: {} in TRACKS table".format(track))


class ApiException(Exception):
    def __init__(self, lap_name):
        Exception.__init__(self, "API Internal Server Error: in lap {}".format(lap_name))


class DynamoDbBadStatusCode(Exception):
    def __init__(self, statusCode: int):
        message = f"DynamoDb commend reDturn status code {statusCode}"
        super().__init__(message)


class KwargsMissingArgException(Exception):
    def __init__(self, function: str, missingArgs: []):
        separator: str = ', '
        message = f"missing args exception function {function} missing args :" \
                  f"{separator.join(missingArgs)}"
        super().__init__(message)


class KpiLambdaError(Exception):
    def __init__(self, acc: str, e: Exception = None):
        message = f"lambda that calculate kpi for account {acc} raise error: \n {e}"
        super().__init__(message)
