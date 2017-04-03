import Queue

class BufferQueue(Queue.Queue):

    def __iter__(self):
        return iter(self.get, None)

    def close(self):
        self.put(None)
