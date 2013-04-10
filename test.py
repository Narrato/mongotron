
import pymongo
import mongotron


conn = pymongo.Connection()
mongotron.GetConnectionManager().add_connection(conn)


class Doc(mongotron.Document):
    __db__ = 'test'
    structure = {
        'name': unicode,
        'age': int,
        'events': [int]
    }


d = Doc()
d.age = 103
#d.age = "dave"
d.save()
pprint((d))

print d.age
