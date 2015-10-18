import os
from urlparse import parse_qs

from webob import Response
from webob.exc import HTTPNotFound

from .node import Node
from .volume import Volume

node = Node('/tmp/boo')
node.add_volume(Volume('boo', '/tmp/boo'))


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
            node.put(volume, collection, args['key'][0], env['wsgi.input'])
            res = Response('Ok')
        elif method == 'GET':
            fname, meta = node.get(volume, collection, args['key'][0])
            res = Response()
            res.headers['X-Sendfile'] = fname

    if not res:
        res = HTTPNotFound()

    return res(env, start_response)
