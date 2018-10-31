__author__ = 'maesker'




class Event:
    __slots__ = ['id','name','attributes','get']
    def __init__(self, id, name, attributes):
        self.id = id
        self.name = name
        self.attributes = attributes
        self.get = self.attributes.get


    def get_id(self):
        return self.id

    def set(self, attribute, value):
        self.attributes[attribute] = value

    def get(self, attribute):
        return self.attributes.get(attribute, None)

    def get_time(self):
        return self.attributes['datetime']

    def get_name(self):
        return self.name

    def __repr__(self):
        return "ID:%08i;name:%27s;%s" % (self.id, self.name, self.attributes)
        #s = ""
        #for i in ['cartridgeid', 'driveid', 'libraryid']:
        #    if i in self.attributes:
        #        s += "%s:%s" % (i, self.attributes[i])
        #return "ID:%07i;name:%30s;%s-%s" % (self.id, self.name,
        #                                    self.attributes['datetime'], s)
