
from __future__ import absolute_import
from pymongo.cursor import Cursor as PymongoCursor

class Cursor(PymongoCursor):

    def __init__(self, *args, **kwargs):
        self.__wrap = None
        if kwargs:
            self.__wrap = kwargs.pop('document_class', None)
        super(Cursor, self).__init__(*args, **kwargs)

    def next(self):
        document = self._cursor.next()
        return self._document_class(doc=document)

    def next(self):
        if self._Cursor__empty:
            raise StopIteration

        obj = super(Cursor, self).next()

        if (self.__wrap is not None) and isinstance(obj, dict):
            return self.__wrap(obj)
        return obj

    def __getitem__(self, index):
        obj = super(Cursor, self).__getitem__(index)
        if (self.__wrap is not None) and isinstance(obj, dict):
            return self.__wrap(obj)
        return obj
