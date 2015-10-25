class Volume(object):
    def __init__(self, id, weight=1):
        self.id = id
        self.weight = weight

    def __repr__(self):
        return 'Volume({}, {})'.format(self.id, self.weight)
