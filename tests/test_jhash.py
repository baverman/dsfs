from __future__ import division

import time, random
from collections import OrderedDict, defaultdict, Counter
from array import array

from dsfs.jhash import jhash, rand, lt, straw
from dsfs.hashring import HashRing, PARTITION_COUNT, partition


def wrap(v, max):
    while v > max:
        v -= max

    return v


rnd = array('H')
rnd.fromfile(open('/tmp/rnd.bin'), 7993)


class Straw(object):
    def __init__(self, items):
        self.values = [r[0] for r in items]
        self.choices = array('l', [int(r[1] * 10) for r in items])

    def get(self, key):
        return self.values[straw(key, self.choices)]


class JHash(object):
    def __init__(self, partitions, replicas, volumes):
        self.partitions = partitions
        self.replicas = replicas
        self.volumes = list(volumes)
        self.nodes = {}

        groups = OrderedDict()
        nodes = {}
        sizes = Counter()
        total = 0
        for volume, weight, node, group in self.volumes:
            nn = groups.setdefault(group, OrderedDict()).setdefault(node, [[], 0])
            nn[0].append((volume, weight))
            nn[1] += weight
            nodes.setdefault(group, []).append((volume, node))
            self.nodes[volume] = node, group
            sizes[group] += weight
            total += weight

        for g in groups.itervalues():
            for node in g.itervalues():
                node[0] = Straw(node[0])

        # t = time.time()
        # for g, group in groups.iteritems():
        #     pnodes = {}
        #     for n in set(r[1] for r in nodes[g]):
        #         pnodes[n] = [r for r in nodes[g] if r[1] != n]

        #     glen = len(group)
        #     for i, (_, node, _) in enumerate(group):
        #         volumes = pnodes[node]
        #         if volumes:
        #             group[i][2] = volumes[jhash(i+1, len(volumes))]
        # print time.time() - t

        self.ring = HashRing()
        items = sizes.items()
        at = 0
        for g, size in items[:-1]:
            at += int(size / total * self.partitions)
            self.ring.add(Straw(groups[g].values()), at)
        self.ring.add(Straw(groups[items[-1][0]].values()), 0)
        self.groups = groups

    def add_volumes(self, volumes):
        return JHash(self.partitions, self.replicas, self.volumes + list(volumes))

    def get_volumes(self, key):
        result = []
        step = self.partitions / self.replicas
        for r in xrange(self.replicas):
            k = (key + int(step * r)) % self.partitions
            g = self.ring.get(k)
            node = g.get(key * 7993)
            v = node.get(k)
            result.append(v)

        return result


class CHash(object):
    def __init__(self, replicas, volumes):
        self.replicas = replicas
        self.volumes = list(volumes)
        self.nodes = {}

        total = 0
        ring = []
        nrings = {}
        pp = set()
        t = time.time()
        for volume, weight, node, group in self.volumes:
            total += weight
            self.nodes[volume] = node, group
            for idx in xrange(weight):
                key = '{}{}'.format(volume, idx)
                p = partition(key)
                if p not in pp:
                    pp.add(p)
                    ring.append((p, ([volume], node, group)))

        self.ring = HashRing(ring)
        print time.time() - t

        # groups = groups.items()

        # self.gring = HashRing()
        # self.offsets = {}
        # at = 0
        # for g, w in groups[:-1]:
        #     self.offsets[g] = at
        #     at += w * replicas / total
        #     self.gring.add(g, at)

        # self.offsets[groups[-1][0]] = at
        # self.gring.add(groups[-1][0], 0)

        # self.fill_replicas()

    def fill_replicas(self):
        t = time.time()
        gring = self.gring
        grings = self.grings
        nodes = self.nodes
        rcount = self.replicas

        for k in self.ring.nodes:
            cnode = k.value
            volumes, node, group = cnode
            start = self.offsets[group] + k / PARTITION_COUNT + 1
            vnodes = {node}
            for idx in xrange(rcount - 1):
                g = gring.get(wrap(start + idx, rcount))
                n = node
                while n in vnodes:
                    v, k = grings[g].getnext(k)
                    n, _ = nodes[v]
                volumes.append(v)
                vnodes.add(n)
        print time.time() - t

    def get_volumes(self, key):
        return self.ring[key][0]

    def add_volumes(self, volumes):
        return CHash(self.replicas, self.volumes + list(volumes))


def tes_distribution():
    volumes = []
    vidx = 0
    nidx = 0
    for g, n in [('g1', 30), ('g2', 20), ('g3', 10)]:
        for _ in xrange(n):
            nidx += 1
            for __ in xrange(12):
                vidx += 1
                w = 10
                # if vidx in (532, 433, 544, 484, 578, 552, 509):
                #     w = int(w * 1.5)
                # elif vidx in (713, 605, 704, 611, 682, 623):
                #     w = int(w / 1.5)
                volumes.append(('v{}'.format(vidx), w, 'n{}'.format(nidx), g))

    h = CHash(3, volumes)

    nodes = Counter()
    groups = Counter()
    volumes = Counter()
    same_nodes = 0

    for k in xrange(300000):
        nn = set()
        for v in h.get_volumes('{}'.format(k))[:1]:
            n, g = h.nodes[v]
            nn.add(n)
            nodes[n] += 1
            groups[g] += 1
            volumes[v] += 1

        if len(nn) != 3:
            same_nodes += 1

    sizes = Counter()
    for i, n in enumerate(h.ring.nodes):
        k = n
        if k == 0:
            k == PARTITION_COUNT
        pk = h.ring.nodes[(i - 1) % len(h.ring.nodes)]
        if pk > k:
            k += PARTITION_COUNT
        sizes[n.value[0][0]] += k - pk

    print_counter(sizes)
    print_counter(volumes)
    print_counter(nodes)
    print groups.items()
    print same_nodes
    assert False


sort = lambda r: r[1]
def print_counter(counter):
    items = sorted(counter.items(), key=sort)
    print items[:3], '...', items[-3:]


def test_distribution():
    volumes = []
    vidx = 0
    nidx = 0
    for g, n in [('g1', 30), ('g2', 20), ('g3', 10)]:
        for _ in xrange(n):
            nidx += 1
            for __ in xrange(12):
                vidx += 1
                w = 1
                volumes.append(('v{}'.format(vidx), w, 'n{}'.format(nidx), g))

    h = JHash(150000, 3, volumes)

    nodes = Counter()
    groups = Counter()
    volumes = Counter()
    same_nodes = 0

    t = time.time()
    for k in xrange(150000):
        nn = set()
        for v in h.get_volumes(k)[2:]:
            # if v == 'v426':
            #     print k
            n, g = h.nodes[v]
            nn.add(n)
            nodes[n] += 1
            groups[g] += 1
            volumes[v] += 1

        if len(nn) != 3:
            same_nodes += 1
    print time.time() - t

    maxh = sorted(volumes.items(), key=sort)[-1][1]

    for g, nn in h.groups.iteritems():
        for n, vv in nn.iteritems():
            for v in vv[0].values:
                print g, n, v, '\t', '*' * int(volumes[v] / maxh * 20)
    

    # c = Counter()
    # for k in xrange(100):
    #     c[h.groups['g3']['n60'][0].get(k)] += 1
    # print_counter(c)
    # assert False

    print_counter(volumes)
    print_counter(nodes)
    print groups.items()
    print same_nodes
    assert False


def tes_straw():
    s = Straw([('v1', 1), ('v2', 1), ('v3', 1), ('v4', 2), ('v5', 1), ('v6', 1)])
    c = Counter()
    t = time.time()
    for k in xrange(100):
        c[s.get(k)] += 1
    print time.time() - t

    print_counter(c)
    assert False
