import asyncio


class AsyncLoopManager(object):
    """
    async manager handle all the concurrency  heavy lifting like async io and threading
    """

    def __init__(self):
        self.loop = asyncio.get_event_loop()

    def __enter__(self):
        """

        Returns
        -------
        async_io event loop
        """
        if self.loop.is_running():
            pass  # Todo handle running event loop
        if self.loop.is_closed():
            return self.loop
        return self.loop

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        close the async_io event loop
        Parameters
        ----------
        exc_type
        exc_val
        exc_tb

        """
        self.loop.close()
