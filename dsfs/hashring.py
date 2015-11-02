from bisect import insort, bisect
from hashlib import sha1
from mmh3 import hash as mmhash

PARTITION_COUNT = 0xfffffff + 1


def keyhash(key):
    return sha1(key).hexdigest()


def partition(key):
    return mmhash(key) & 0xfffffff
    # return int(keyhash(key)[:5], 16)


def keysplit(key):
    khash = keyhash(key)
    return khash[:5], khash[5:]


class Node(int): pass
def node(key, value):
    n = Node(key)
    n.value = value
    return n


class HashRing(object):
    def __init__(self, items=None):
        if items:
            self.nodes = [node(k, v) for k, v in items]
            self.nodes.sort()
        else:
            self.nodes = []

    def add(self, value, at):
        if at not in self.nodes:
            insort(self.nodes, node(at, value))

    def get(self, at):
        idx = bisect(self.nodes, at) % len(self.nodes)
        return self.nodes[idx].value

    def getnext(self, at):
        count = len(self.nodes)
        idx = bisect(self.nodes, at) % count
        nidx = (idx + 1) % count
        return self.nodes[idx].value, self.nodes[nidx]

    def __getitem__(self, key):
        return self.get(partition(key))

    def __setitem__(self, key, node):
        self.add(node, partition(key))
