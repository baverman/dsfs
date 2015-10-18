from bisect import insort, bisect
from .utils import partition


class HashRing(object):
    def __init__(self):
        self.keys = []
        self.nodes = {}

    def add(self, node, at):
        if at in self.nodes:
            raise Exception('Node at {} already exists'.format(at))
        self.nodes[at] = node
        insort(self.keys, at)

    def get(self, at):
        idx = bisect(self.keys, at)
        if idx == len(self.keys):
            idx = 0
        return  self.nodes[self.keys[idx]]

    def __getitem__(self, key):
        return self.get(partition(key, self.pnum))
