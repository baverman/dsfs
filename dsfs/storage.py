from urlparse import parse_qs
from cStringIO import StringIO

import uwsgi
from webob import Response
from webob.exc import HTTPNotFound

from .node import Node
from .volume import Volume

node = Node('/tmp/boo')
node.add_volume(Volume('boo', '/tmp/boo'))

RSIZE = 16384
WSIZE = 1 << 18  # 256k


def put_file(env, start_response, volume, collection, key):
    f = env['wsgi.input']
    fd = f.fileno()
    toread = uwsgi.cl()
    if toread < RSIZE:
        fobj = f
    else:
        buf = StringIO()
        sz = 0
        tmpname = node.tmpname(volume)
        tmpf = None
        while toread > 0:
            yield uwsgi.wait_fd_read(fd, 30)
            data = f.read(min(toread, RSIZE))
            rbytes = len(data)
            toread -= rbytes
            sz += rbytes
            buf.write(data)
            if sz > WSIZE:
                if not tmpf:
                    tmpf = open(tmpname, 'wb')
                tmpf.write(buf.getvalue())
                buf = StringIO()
                sz = 0

        if tmpf:
            if sz:
                tmpf.write(buf.getvalue())
            tmpf.close()
            fobj = tmpname
        else:
            buf.seek(0)
            fobj = buf

    node.put(volume, collection, key, fobj)

    start_response('200 OK', [('Content-Type', 'text/plain')])
    yield 'Ok'


def application(env, start_response):
    res = None
    path = env['PATH_INFO'].lstrip('/')
    method = env['REQUEST_METHOD']
    args = parse_qs(env.get('QUERY_STRING', ''))
    if path.startswith('volume/'):
        parts = path.split('/')
        volume = parts[1]
        collection = parts[2]
        if method == 'PUT':
            return put_file(env, start_response,
                            volume, collection, args['key'][0])
        elif method == 'GET':
            fname, meta = node.get(volume, collection, args['key'][0])
            res = Response()
            res.headers['X-Sendfile'] = fname

    if not res:
        res = HTTPNotFound()

    return res(env, start_response)
