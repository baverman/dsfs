import os
from webob import Request, Response
from webob.exc import HTTPNotFound

from .node import Node
from .volume import Volume

node = Node('/tmp/boo')
node.add_volume(Volume('boo', '/tmp/boo'))


def application(env, start_response):
    req = Request(env)
    res = None
    if req.path.startswith('/volume/'):
        volume = req.path.split('/')[2]
        if req.method == 'PUT':
            node.put(volume, req.GET['collection'], req.GET['key'], req.body_file)
            res = Response('Ok')
        elif req.method == 'GET':
            fname, meta = node.get(volume, req.GET['collection'], req.GET['key'])
            res = Response(app_iter=env['wsgi.file_wrapper'](open(fname, 'rb'), 1 << 12))

    if not res:
        res = HTTPNotFound()

    return res(env, start_response)
