#from MongoEngine

import weakref

class ChangeTrackingDict(dict):
    """A special dict so we can watch any changes
    """

    _parent = None
    _name = None

    def __init__(self, dict_items, parent, name):
        self._parent = weakref.proxy(parent)
        self._name = name
        return super(ChangeTrackingDict, self).__init__(dict_items)

    def __setitem__(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).__setitem__(*args, **kwargs)

    def __delete__(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).__delete__(*args, **kwargs)

    def __delitem__(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).__delitem__(*args, **kwargs)

    def __delattr__(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).__delattr__(*args, **kwargs)

    def __getstate__(self):
        self.instance = None
        self._dereferenced = False
        return self

    def __setstate__(self, state):
        self = state
        return self

    def clear(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).clear(*args, **kwargs)

    def pop(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).pop(*args, **kwargs)

    def popitem(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).popitem(*args, **kwargs)

    def update(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingDict, self).update(*args, **kwargs)

    def _mark_as_changed(self):
        if hasattr(self._parent, '_mark_as_changed'):
            self._parent._mark_as_changed(self._name)
