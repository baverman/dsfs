import lmdb
from msgpack import dumps, loads

from .utils import keysplit

DBSIZE = 1 << 30  # 1Gb


class Node(object):
    def __init__(self, dbpath, volumes=None):
        self.db = lmdb.open(dbpath, map_size=DBSIZE)
        self.volumes = {r.id: r for r in volumes or []}

    def add_volume(self, volume):
        self.volumes[volume.id] = volume

    def put(self, volume, collection, key, fileobj, meta=None):
        self.volumes[volume].put(collection, key, fileobj)
        partition, _ = keysplit(key)
        with self.db.begin(write=True) as tx:
            tx.put('{}:{}:{}'.format(partition, collection, key),
                   dumps(meta or {}))

    def get(self, volume, collection, key):
        partition, _ = keysplit(key)
        with self.db.begin() as tx:
            data = tx.get('{}:{}:{}'.format(partition, collection, key), None)

        if data:
            data = loads(data)
            return self.volumes[volume].get(collection, key), data

        return None, None
