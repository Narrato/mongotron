
from __future__ import absolute_import

from pymongo.database import Database as PymongoDatabase
from .Collection import Collection


class Database(PymongoDatabase):
    def __init__(self, *args, **kwargs):
        self._collections = {}
        super(Database, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        if not key in self._collections:
            self._collections[key] = Collection(self, key)
        return self._collections[key]
