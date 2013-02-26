from pymongo.cursor import Cursor as PymongoCursor

class Cursor(PymongoCursor):
    def __init__(self, document_class, cursor):
        self.__dict__['_document_class'] = document_class
        self.__dict__['_cursor'] = cursor
        
    def __getattr__(self,key):
        if hasattr(self._cursor,key):
            return getattr(self._cursor, key)
        raise AttributeError
        
    def __setattr__(self,key,value):
        setattr(self._cursor,key,value)

    def __iter__(self):
        return self

    def next(self):
        document = self._cursor.next()
        return self._document_class(doc=document)
        
    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.__class__(self._document_class, self._cursor.__getitem__(key))
        document = self._cursor[key]
        return self._document_class(doc=document)
