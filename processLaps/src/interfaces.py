# Iclassifier interface for classifier class
# every classifier need to have classify function


class Iclassifier:
    # classify is function that get Lap and anther params if needed
    # and classify the lap and return the class of the lap
    def classify(self, lap, **kwargs) -> str:
        pass


"""
@class IApiWrapper is interface for ApiWrapper
"""


class IApiWrapper:

    @classmethod
    def get(cls, url, **kwargs):
        pass

    @classmethod
    def put(cls, url, **kwargs):
        pass

    @classmethod
    def post(cls, url, **kwargs):
        pass
