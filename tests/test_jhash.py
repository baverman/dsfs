from dsfs.jhash import jhash


def speed():
    for k in xrange(1, 10000000):
        jhash(k, 100000)


def distribution():
    for k in xrange(1, 1000000):
        print jhash(k, 65535)
