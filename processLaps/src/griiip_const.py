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


classifications = ClassificationConst()

