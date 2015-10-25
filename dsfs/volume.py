import os.path
import shutil
from uuid import uuid4

import lmdb
from msgpack import dumps, loads

from .utils import ensure_path
from .hashring import keysplit

DBSIZE = 1 << 29  # 512MB


class Volume(object):
    def __init__(self, id, path, weight=1):
        self.id = id
        self.weight = weight
        self.db = lmdb.open(path, map_size=DBSIZE)
        self.path = path
        self.tmppath = os.path.join(self.path, 'tmp')
        if not os.path.exists(path):
            raise Exception('Path not found: {}'.format(path))
        ensure_path(self.tmppath)

    def keypath(self, collection, key):
        partition, tail = keysplit(key)
        return partition, os.path.join(self.path, collection, tail[:3], tail[3:6])

    def tmpname(self):
        return os.path.join(self.tmppath, uuid4().hex)

    def put(self, collection, key, fileobj, meta=None):
        partition, dirname = self.keypath(collection, key)
        ensure_path(dirname)

        fname = os.path.join(dirname, key)
        if isinstance(fileobj, str):
            os.rename(fileobj, fname)
        else:
            with open(fname, 'wb') as dst:
                shutil.copyfileobj(fileobj, dst)

        with self.db.begin(write=True) as tx:
            tx.put('{}:{}:{}'.format(partition, collection, key),
                   dumps(meta or {}))

    def get(self, collection, key):
        partition, dirname = self.keypath(collection, key)

        with self.db.begin() as tx:
            data = tx.get('{}:{}:{}'.format(partition, collection, key), None)

        if data:
            data = loads(data)
            return os.path.join(dirname, key), data

        return None, None

    def __repr__(self):
        return 'Volume({}, {})'.format(self.id, self.path)
