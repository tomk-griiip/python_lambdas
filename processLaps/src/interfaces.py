import abc

from griiip_exeptions import InterfaceImplementationException


class IApiWrapper(metaclass=abc.ABCMeta):
    """
    class IApiWrapper is interface for ApiWrapper
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'get') and
                callable(subclass.get) and
                hasattr(subclass, 'post') and
                callable(subclass.post) and
                hasattr(subclass, 'put') and
                callable(subclass.put)
                or
                NotImplemented)

    @abc.abstractmethod
    def get(self, url, **kwargs):
        """
        use for get request
        Parameters
        ----------
        url
        the service part of the end point
        kwargs
        url params
        Returns
        -------
        the response from the end point
        """
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, url, **kwargs):
        """
        use for put request
        Parameters
        ----------
        url
        the service part of the end point
        kwargs
        url params
        Returns
        -------
        the response from the end point
        """
        raise NotImplementedError

    @abc.abstractmethod
    def post(self, url, **kwargs):
        """
        use for post request
        Parameters
        ----------
        url
        the service part of the end point
        kwargs
        url params
        Returns
        -------
        the response from the end point
        """
        raise NotImplementedError


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


class IDb(metaclass=abc.ABCMeta):
    """
    Interface for class do crud ops on db
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'getRunData') and
                callable(subclass.getRunData) and
                hasattr(subclass, 'updateDriverLap') and
                callable(subclass.updateDriverLap) and
                callable(subclass.query) and
                hasattr(subclass, 'query') and
                callable(subclass.insert) and
                hasattr(subclass, 'insert') and
                callable(subclass.update) and
                hasattr(subclass, 'update') and
                callable(subclass.delete) and
                hasattr(subclass, 'delete') or
                NotImplemented)

    @abc.abstractmethod
    def getRunData(self, lapId, **kwargs) -> {}:
        """
        Methods that get data from run datat table
        Parameters
        ----------
        lapId
        kwargs

        Returns
        -------
        python dict
        """
        raise NotImplementedError

    @abc.abstractmethod
    def updateDriverLap(self, columns_to_update: {}, lap_name: str) -> bool:
        """
        Parameters
        ----------
        columns_to_update column withe their value to be update
        lap_name the lap to update

        Returns
        -------
        True for SUCCESS and False for FAILURE
        """
        raise NotImplementedError

    @abc.abstractmethod
    def query(self, sql_cmd):
        """

        Parameters
        ----------
        sql_cmd

        Returns
        -------

        """
        raise NotImplementedError

    @abc.abstractmethod
    def insert(self, sql_cmd):
        """

        Parameters
        ----------
        sql_cmd

        Returns
        -------

        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, sql_cmd):
        """

        Parameters
        ----------
        sql_cmd

        Returns
        -------

        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, sql_cmd):
        """

        Parameters
        ----------
        sql_cmd

        Returns
        -------

        """
        raise NotImplementedError
