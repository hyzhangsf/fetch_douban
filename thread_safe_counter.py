class Counter:

    """
        a thread safe counter class
    """

    def __init__(self, start):

        self._count = start
        self.lock = threading.Lock()
        self.stopped = False
        self.epilog = None

    def step(self):
        self.lock.acquire()
        self._count += 1
        self.lock.release()

    @property
    def count(self):
        c = None
        self.lock.acquire()
        c = self._count
        self.lock.release()
        return c

    def stop(self):
        self.stopped = True

    def set_epilog(self, words):
        self.epilog = words

    def get_epilog(self, words):
        return self.epilog
