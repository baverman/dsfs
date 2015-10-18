import os.path
import shutil

from .utils import ensure_path, keysplit


class Volume(object):
    def __init__(self, id, path):
        self.id = id
        self.path = path
        if not os.path.exists(path):
            raise Exception('Path not found: {}'.format(path))

    def keypath(self, collection, key):
        _, p = keysplit(key)
        return os.path.join(self.path, collection, p[:3], p[3:6])

    def put(self, collection, key, fileobj):
        dirname = self.keypath(collection, key)
        ensure_path(dirname)
        with open(os.path.join(dirname, key), 'wb') as dst:
            shutil.copyfileobj(fileobj, dst)

    def get(self, collection, key):
        dirname = self.keypath(collection, key)
        return os.path.join(dirname, key)
