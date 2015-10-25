import os.path
from urlparse import parse_qsl
from cStringIO import StringIO
from zlib import crc32

import uwsgi
from webob import Response
from webob.exc import HTTPNotFound, HTTPTemporaryRedirect
from webob.multidict import MultiDict

from . import config

cluster = config.get_cluster_from_config('dsfs.conf')
current_node = cluster.find_node(os.environ['DSFS_NODE'])

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
            if v.node is current_node and v.id in current_node.volumes:
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
    else:
        collection = parts[0]
        key = args['key']
        v = volumes[0]
        res = HTTPTemporaryRedirect(
            location='http://{}/{}?key={}&volume={}'.format(v.node.id, collection, key, v.id))
        for v in volumes:
            res.headers.add(
                'X-Location',
                'http://{}/{}?key={}&volume={}'.format(v.node.id, collection, key, v.id))

    if not res:
        res = HTTPNotFound()

    return res(env, start_response)
