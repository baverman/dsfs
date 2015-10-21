import os.path
from urlparse import parse_qsl
from cStringIO import StringIO

import uwsgi
from webob import Response
from webob.exc import HTTPNotFound
from webob.multidict import MultiDict

from .node import Node
from .volume import Volume

node = Node()
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
        tmpname = volume.tmpname()
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

    meta = {}
    if 'CONTENT_TYPE' in env:
        meta['ct'] = env['CONTENT_TYPE']

    volume.put(collection, key, fobj, meta)

    start_response('200 OK', [('Content-Type', 'text/plain')])
    yield 'Ok'


def application(env, start_response):
    res = None
    path = env['PATH_INFO'].lstrip('/')
    method = env['REQUEST_METHOD']
    args = MultiDict(parse_qsl(env.get('QUERY_STRING', '')))

    parts = path.split('/')
    volume = args.get('volume')
    if volume:
        v = node.volumes[volume]
        collection = parts[0]
        if method == 'PUT':
            return put_file(env, start_response,
                            v, collection, args['key'])
        elif method == 'GET':
            fname, meta = v.get(collection, args['key'])
            res = Response()
            res.charset = None
            res.headers['X-Sendfile'] = fname
            res.content_length = os.path.getsize(fname)
            if 'ct' in meta:
                res.content_type = meta['ct']

    if not res:
        res = HTTPNotFound()

    return res(env, start_response)
