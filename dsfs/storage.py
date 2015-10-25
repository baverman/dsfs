import os.path
from urlparse import parse_qsl
from cStringIO import StringIO
from zlib import crc32

import uwsgi
from webob import Response
from webob.exc import HTTPNotFound
from webob.multidict import MultiDict

from .cluster import Cluster, Node
from .volume import Volume

current_node = Node('n1')
current_node.add_volume(Volume('v1', '/tmp/v1'))
current_node.add_volume(Volume('v2', '/tmp/v2'))

cluster = Cluster(1)
cluster.add_node('g1', current_node)


RSIZE = 16384
WSIZE = 1 << 18  # 256k


def put_file(env, start_response, volume, collection, key):
    f = env['wsgi.input']
    fd = f.fileno()
    toread = uwsgi.cl()
    tmpf = None
    crc = 0
    if toread < RSIZE:
        data = f.read()
        crc = crc32(data, crc)
        buf = StringIO(data)
    else:
        buf = StringIO()
        sz = 0
        tmpname = volume.tmpname()
        while toread > 0:
            yield uwsgi.wait_fd_read(fd, 30)
            data = f.read(min(toread, RSIZE))
            rbytes = len(data)
            toread -= rbytes
            sz += rbytes
            crc = crc32(data, crc)
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

    meta = {'crc': crc & 0xffffffff}
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
    if not volume:
        volumes = cluster.get_volumes(args['key'])
        for v in volumes:
            if v.id in current_node.volumes:
                volume = v.id
                break

    if volume:
        v = current_node.volumes[volume]
        collection = parts[0]
        if method == 'PUT':
            return put_file(env, start_response,
                            v, collection, args['key'])
        elif method == 'GET':
            fname, meta = v.get(collection, args['key'])
            res = Response()
            res.charset = None
            res.headers['X-Sendfile'] = fname
            res.headers['X-Crc'] = str(meta['crc'])
            res.content_length = os.path.getsize(fname)
            if 'ct' in meta:
                res.content_type = meta['ct']

    if not res:
        res = HTTPNotFound()

    return res(env, start_response)
