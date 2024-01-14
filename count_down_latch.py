import asyncio


class CountDownLatch(object):
    def __init__(self, count=1):
        self.count = count
        # had to use two locks, because for some reason Condition was blocking other coroutines
        self.lock = asyncio.Lock()
        self.condition = asyncio.Condition(lock=self.lock)

    async def count_down(self):
        await self.lock.acquire()
        try:
            self.count -= 1
            print("Count down:", self.count)
            if self.count <= 0:
                self.condition.notify_all()
        finally:
            self.lock.release()

    async def tasks_await(self):
        await self.lock.acquire()
        try:
            while self.count > 0:
                print("Waiting for tasks:", self.count)
                await self.condition.wait()
        finally:
            self.lock.release()
