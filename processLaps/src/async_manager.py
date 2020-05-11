import asyncio

"""
async manager handle all the concurrency  heavy lifting like async io and threading

"""


class AsyncLoopManager(object):
    def __init__(self):
        self.loop = asyncio.get_event_loop()

    def __enter__(self):
        if self.loop.is_running():
            pass
        if self.loop.is_closed():
            return self.loop
        return self.loop

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.loop.close()
