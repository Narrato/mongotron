
from __future__ import absolute_import
from pymongo.collection import Collection as PymongoCollection
from .Cursor import Cursor

class Collection(PymongoCollection):

    def find(self, *args, **kwargs):
        return Cursor(self, *args, **kwargs)
