
from collections import deque

PT_STATE_EMPTY = "empty"
PT_STATE_ALLOCATED = "alloc"
PT_STATE_LOADED = "loaded"


class BaseClassLevelmigration:
    def __init__(self, id):
        self.id = id,
        self.allocated = None
        self.state = PT_STATE_EMPTY
        self.object = None
        self.reachable = deque()

    def allocate(self, crtid):
        self.allocated = crtid
        self.state = PT_STATE_ALLOCATED

    def deallocate(self):
        self.allocated = None
        self.state = PT_STATE_EMPTY

    def put(self, obj):
        if self.object == None:
            self.object = obj
        else:
            raise Exception("Passthru not empty")

    def get(self, delete):
        obj = self.object
        if obj:
            if delete:
                self.object = None
                self.deallocate()
        return obj

    def isfree(self):
        if not self.allocated:
            return 1
        return 0

class PassThru(BaseClassLevelmigration):
    def __init__(self, id, left, right):
        BaseClassLevelmigration.__init__(self,id)
        self.left_levelid = left
        self.right_levelid = right
        self.reachable = deque([self.right_levelid,self.left_levelid])

class Elevator(BaseClassLevelmigration):
    def __init__(self, id, pos, id_as_int):
        BaseClassLevelmigration.__init__(self,id)
        self.pos = pos
        self.reachable = deque([id_as_int, id_as_int+1, id_as_int+2, id_as_int+3])
