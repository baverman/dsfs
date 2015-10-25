from __future__ import division
from collections import OrderedDict

from .hashring import HashRing, PARTITION_COUNT, partition
from .utils import cached_property


class Cluster(object):
    def __init__(self, rcount=3):
        self.rcount = 3
        self.groups = OrderedDict()

    def find_node(self, node):
        for r in self.groups.values():
            if node in r:
                return r[node]

    def add_node(self, group, node):
        self.groups.setdefault(group, OrderedDict())[node.id] = node
        try:
            del self.ring
        except AttributeError:
            pass

    def get_volumes(self, key):
        p = partition(key)
        d = PARTITION_COUNT / self.rcount
        return [self.ring.get(int(p + d * r + 0.5) % PARTITION_COUNT)
                for r in xrange(self.rcount)]

    def iter_volumes(self):
        for r in self.groups.values():
            for n in r.values():
                for v in n.volumes.values():
                    yield v

    @cached_property
    def ring(self):
        total_weight = sum(v.weight for v in self.iter_volumes())
        r = HashRing()
        cw = 0
        for v in self.iter_volumes():
            cw += v.weight
            r.add(v, int(cw / total_weight * PARTITION_COUNT + 0.5) % PARTITION_COUNT)

        return r


class Node(object):
    def __init__(self, id, volumes=None):
        self.id = id
        self.volumes = OrderedDict((r.id, r) for r in volumes or [])

    def add_volume(self, volume):
        volume.node = self
        self.volumes[volume.id] = volume

    def __repr__(self):
        return 'Node({})'.format(self.id)
