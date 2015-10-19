import os.path
import shutil
from uuid import uuid4

from .utils import ensure_path, keysplit


class Volume(object):
    def __init__(self, id, path):
        self.id = id
        self.path = path
        self.tmppath = os.path.join(self.path, 'tmp')
        if not os.path.exists(path):
            raise Exception('Path not found: {}'.format(path))
        ensure_path(self.tmppath)

    def keypath(self, collection, key):
        _, p = keysplit(key)
        return os.path.join(self.path, collection, p[:3], p[3:6])

    def tmpname(self):
        return os.path.join(self.tmppath, uuid4().hex)

    def put(self, collection, key, fileobj):
        dirname = self.keypath(collection, key)
        ensure_path(dirname)
        fname = os.path.join(dirname, key)
        if isinstance(fileobj, str):
            os.rename(fileobj, fname)
        else:
            with open(fname, 'wb') as dst:
                shutil.copyfileobj(fileobj, dst)

    def get(self, collection, key):
        dirname = self.keypath(collection, key)
        return os.path.join(dirname, key)
