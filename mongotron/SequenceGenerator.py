
from __future__ import absolute_import
from .ConnectionManager import GetConnectionManager

class SequenceGenerator(object):
    
    @classmethod
    def get_next_index(cls, object, database_name, collection_name, connection_name=None):
        name = object.__class__.__name__
        
        connection = GetConnectionManager().get_connection(connection_name, True)
        
        collection = connection[database_name][collection_name]

        # we *DO* ensure an index here, it might be important
        collection.ensure_index("name")

        new_id = collection.find_and_modify(query={"name":name},
                                            update={"$inc":{"seq":long(1)}},
                                            new=True,
                                            upsert=True).get("seq")

        return long(new_id)
