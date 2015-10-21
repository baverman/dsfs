from StringIO import StringIO

from dsfs.volume import Volume


def test_putget(tmpdir):
    v = Volume('foo', str(tmpdir.mkdir('foo')))

    v.put('txt', '1', StringIO('text'), {'content-type': 'plain/text'})
    fname, meta = v.get('txt', '1')
    assert meta == {'content-type': 'plain/text'}
    assert open(fname).read() == 'text'

    # double put
    v.put('txt', '1', StringIO('{"boo": "bar"}'),
          {'content-type': 'application/json'})
    fname, meta = v.get('txt', '1')
    assert meta == {'content-type': 'application/json'}
    assert open(fname).read() == '{"boo": "bar"}'

    # non existing key
    fname, meta = v.get('txt', '2')
    assert meta is None
    assert fname is None
