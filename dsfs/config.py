from collections import OrderedDict
from ConfigParser import ConfigParser

from .cluster import Node, Cluster
from .volume import Volume


def get_cluster_from_config(fname):
    c = ConfigParser()
    c.read(fname)

    cluster = Cluster(c.getint('cluster', 'replicas'))
    for s in c.sections():
        if s == 'cluster':
            continue

        node = Node(s)
        cluster.add_node(c.get(s, 'group') or 'default', node)

        volumes = OrderedDict()
        for k, value in c.items(s):
            if k.startswith('volume.'):
                _, volume, param = k.split('.', 2)
                volumes.setdefault(volume, {})[param] = value

        for volume, params in volumes.items():
            w = params.get('weight')
            if w:
                w = int(w)
            node.add_volume(Volume(volume, params['path'], w))

    return cluster
