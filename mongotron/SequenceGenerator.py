
from __future__ import absolute_import
from .ConnectionManager import GetConnectionManager


class SequenceGenerator(object):
    
    @classmethod
    def get_next_index(cls, seq_name, database_name, collection_name, connection_name=None):
        # TODO: remove me after a few releases.
        if not isinstance(seq_name, basestring):
            seq_name = seq_name.__class_.__name__

        connection = GetConnectionManager().get_connection(connection_name, True)
        
        collection = connection[database_name][collection_name]

        # we *DO* ensure an index here, it might be important
        collection.ensure_index("name")

        new_id = collection.find_and_modify(query={"name":seq_name},
                                            update={"$inc":{"seq":long(1)}},
                                            new=True,
                                            upsert=True).get("seq")

        return long(new_id)
