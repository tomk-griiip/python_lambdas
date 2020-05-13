from griiip_exeptions import CantConnectToDbException


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
        if not self.is_conned:
            if not self._connect():
                raise CantConnectToDbException
        return func(self, *args, **kwargs)

    return if_not_connect_do_connect
