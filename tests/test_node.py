from StringIO import StringIO

from dsfs.node import Node
from dsfs.volume import Volume


def test_putget(tmpdir):
    node = Node(str(tmpdir))
    node.add_volume(Volume('foo', str(tmpdir.mkdir('foo'))))

    node.put('foo', 'txt', '1', StringIO('text'), {'content-type': 'plain/text'})
    fname, meta = node.get('foo', 'txt', '1')
    assert meta == {'content-type': 'plain/text'}
    assert open(fname).read() == 'text'

    # double put
    node.put('foo', 'txt', '1', StringIO('{"boo": "bar"}'),
             {'content-type': 'application/json'})
    fname, meta = node.get('foo', 'txt', '1')
    assert meta == {'content-type': 'application/json'}
    assert open(fname).read() == '{"boo": "bar"}'

    # non existing key
    fname, meta = node.get('foo', 'txt', '2')
    assert meta is None
    assert fname is None
