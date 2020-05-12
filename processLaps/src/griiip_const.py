import sys


def constant(f):
    def fset(self, value):
        raise TypeError

    def fget(self):
        return f()

    return property(fget, fset)


class ClassificationConst(object):
    @constant
    def COMPETITIVE():
        return 'Competitive'

    @constant
    def PARTIAL():
        return 'Partial'

    @constant
    def NON_COMPETITIVE():
        return 'NonCompetitive'

    @constant
    def NON_SUCCESSFUL():
        return 'Non-Successful'

    @constant
    def NON_LEGIT():
        return 'NonLegit'


class NetConst(object):
    @constant
    def OK():
        return 200

    @constant
    def SUCCESS():
        return True

    @constant
    def FAILURE():
        return False

    @constant
    def UPDATE_DRIVER_LAP_URL():
        return "/driverlaps/"

    @constant
    def RUNDATA_URL():
        return "/rundata/"


class ErrorMessages:
    @constant
    def MYSQL_MISSING_DATA():
        return "LAP IS MISSING DATA IN MYSQL, Exception raised is:"

    @constant
    def MISSING_INTERFACE_FUNCTIONS(self):
        return "object missing mandatory functions that specific in the object interface"


classifications = ClassificationConst()
net = NetConst()
errorMessages = ErrorMessages
