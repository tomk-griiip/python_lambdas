import abc

from griiip_exeptions import InterfaceImplementationException


class IClassifier(metaclass=abc.ABCMeta):
    """
    Interface for class that can classify laps
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'classify') and
                callable(subclass.classify) or
                NotImplemented)

    @abc.abstractmethod
    def classify(self, lap, **kwargs):
        """
        Methods that classify the lap
        Parameters
        ----------
        lap
        kwargs

        Returns
        -------

        """
        raise NotImplementedError


class IDataBaseClient(metaclass=abc.ABCMeta):
    """
    Interface for class to do crud ops on db (class that implement this interface
    is a data base client)
    can be not direct like api that use api get away
    and not some data base client
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'get') and
                callable(subclass.get) and
                hasattr(subclass, 'post') and
                callable(subclass.post) and
                hasattr(subclass, 'put') and
                callable(subclass.put) and
                hasattr(subclass, 'delete') and
                callable(subclass.delete) and
                hasattr(subclass, 'commit') and
                callable(subclass.commit) or
                NotImplemented)

    @abc.abstractmethod
    def get(self, sql_cmd, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def post(self, sql_cmd, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, sql_cmd, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, sql_cmd, **kwargs):
        raise NotImplementedError


class IDataBase(metaclass=abc.ABCMeta):
    """
    Interface for class to do crud ops on db (class that implement this interface
    is a data base client)
    can be not direct like api that use api get away
    and not some data base client
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'getRunData') and
                callable(subclass.getRunData) and
                hasattr(subclass, 'updateDriverLap') and
                callable(subclass.updateDriverLap) and
                hasattr(subclass, 'putLap') and
                callable(subclass.putLap) and
                hasattr(subclass, 'commit') and
                callable(subclass.commit) and
                NotImplemented)

    @abc.abstractmethod
    def getRunData(self, query, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def updateDriverLap(self, update, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def putLap(self, insert, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self, **kwargs):
        raise NotImplementedError
