class Node(object):
    def __init__(self, volumes=None):
        self.volumes = {r.id: r for r in volumes or []}

    def add_volume(self, volume):
        self.volumes[volume.id] = volume
