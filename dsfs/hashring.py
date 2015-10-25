from bisect import insort, bisect
from hashlib import sha1

PARTITION_COUNT = 0xfffff + 1


def keyhash(key):
    return sha1(key).hexdigest()


def partition(key):
    return int(keyhash(key)[:5], 16)


def keysplit(key):
    khash = keyhash(key)
    return khash[:5], khash[5:]


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
        return self.get(partition(key))
