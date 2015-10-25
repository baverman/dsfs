from dsfs.cluster import Cluster, Node

from .helpers import Volume


def test_volume_order():
    c = Cluster(3)
    c.add_node('g1', Node('n1', [Volume('v1'), Volume('v2')]))
    c.add_node('g2', Node('n2', [Volume('v3')]))
    c.add_node('g3', Node('n3', [Volume('v4', 2)]))

    assert [v.id for v in c.iter_volumes()] == ['v1', 'v2', 'v3', 'v4']


def test_ring():
    c = Cluster(3)
    c.add_node('g1', Node('n1', [Volume('v1'), Volume('v2')]))
    c.add_node('g2', Node('n2', [Volume('v3')]))
    c.add_node('g3', Node('n3', [Volume('v4', 2)]))

    assert c.ring.keys == [0, 209715, 419430, 629146]
    assert [c.ring.get(k).id for k in c.ring.keys] == ['v1', 'v2', 'v3', 'v4']

    assert [v.id for v in c.get_volumes('key1')] == ['v1', 'v2', 'v4']
    assert [v.id for v in c.get_volumes('key2')] == ['v3', 'v4', 'v1']
