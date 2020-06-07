from .griiip_exeptions import CantConnectToDbException
import time
from . import logger

def ifNotConnectDo(func):
    """
    decorator fo checking if there is connection if not call _connect function
    each function that use this decorator need to have self._connect function and self.is_conned
    variable that mark if there is connection or not in the self object/function
    Parameters
    ----------
    func the function to be decorated

    Returns
    -------
    the result of the function func

    """

    def if_not_connect_do_connect(self, *args, **kwargs):
        if not self._is_connect():
            if not self._connect():
                raise CantConnectToDbException
        return func(self, *args, **kwargs)

    return if_not_connect_do_connect


def addTable(_dict: {}):
    """
    addTable decorator to add item names to a dict
    Parameters
    ----------
    _dict the list to add item to
    int the *args the first argument need to be the item to add to the list
    or if using **kwargs then need to be in the function name={item}
    Returns
    -------

    """

    def add_table(func):
        def add_table_inner(*args, **kwargs):
            if len(args) > 1:
                n = args[1]
                _dict[n] = n
            elif 'tableName' in kwargs:
                n = kwargs['tableName']
                _dict[n] = n
            return func(*args, **kwargs)

        return add_table_inner

    return add_table


def execution_time(func):
    """
    decorator to test execution time
    :param func:
    :return:
    """
    def inner(*args, **kwargs):
        start_time = time.time()
        func(*args, **args)
        logger.debug(f"--- {time.time() - start_time} execution time in seconds ---")

    return inner
