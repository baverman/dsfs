import errno
import os.path

from hashlib import sha1

PARTITION_COUNT = 0xfffff + 1


def keyhash(key):
    return sha1(key).hexdigest()


def partition(key):
    return int(keyhash(key)[:5], 16)


def keysplit(key):
    khash = keyhash(key)
    return khash[:5], khash[5:]


def ensure_path(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
