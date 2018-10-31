
class SimulationEvent:
    __slots__ = ['prefix', 'priority','name','attributes','get']
    def __init__(self, prefix, prio, name, attributes):
        self.priority = prio
        self.prefix = prefix
        self.name = name
        self.attributes = attributes
        self.get = self.attributes.get


    def get_id(self):
        return "%s-%08i"%(self.prefix, self.priority)

    def set(self, attribute, value):
        self.attributes[attribute] = value

    #def get(self, attribute):
    #    return self.attributes.get(attribute, None)

    def get_time(self):
        return self.attributes.get('datetime', None)

    def get_name(self):
        return self.name

    def __repr__(self):
        s = ""
        for i in ['cartridgeid', 'driveid', 'libraryid']:
            if i in self.attributes:
                s += " %s:%s" % (i, self.attributes[i])
        return "ID:%s;%s:name:%27s;%s" % (self.get_id(), self.get_time(), self.name, s)

    def delete(self, att):
        if att in self.attributes:
            del self.attributes[att]
