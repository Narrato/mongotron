#from MongoEngine

from __future__ import absolute_import
import weakref

#TODO: implement this so it actually does the right thing I think

class ChangeTrackingList(list):
    """A special list so we can watch any changes
    """

    _parent = None
    _name = None

    def __init__(self, list_items, parent, name):
        if parent:
            self._parent = weakref.proxy(parent)
        self._name = name
        return super(ChangeTrackingList, self).__init__(list_items)

    def __setitem__(self, *args, **kwargs):
        self._mark_as_changed()
        #self._add_operation('$set', self._name+'.'+str(args[0]), (args[1]))
        return super(ChangeTrackingList, self).__setitem__(*args, **kwargs)

    def __delitem__(self, *args, **kwargs):
        #db.lists.update({}, {$unset : {"interests.3" : 1 }})
        #db.lists.update({}, {$pull : {"interests" : null}})
        self._mark_as_changed()
        return super(ChangeTrackingList, self).__delitem__(*args, **kwargs)

    def __getstate__(self):
        self.observer = None
        return self

    def __setstate__(self, state):
        self = state
        return self

    def append(self, *args, **kwargs):
        self._mark_as_changed()
        # this is how we'd pass back stuff to do atomic adds
        #self._parent.push(self._name, *args)
        return super(ChangeTrackingList, self).append(*args, **kwargs)

    def extend(self, *args, **kwargs):
        self._mark_as_changed()
        # this is how we'd pass back stuff to do atomic adds
        #self._parent.push(self._name, *args)
        return super(ChangeTrackingList, self).extend(*args, **kwargs)

    def insert(self, *args, **kwargs):
        #print "tracking list - insert"
        self._mark_as_changed()
        return super(ChangeTrackingList, self).insert(*args, **kwargs)

    def pop(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingList, self).pop(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingList, self).remove(*args, **kwargs)

    def reverse(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingList, self).reverse(*args, **kwargs)

    def sort(self, *args, **kwargs):
        self._mark_as_changed()
        return super(ChangeTrackingList, self).sort(*args, **kwargs)

    def _mark_as_changed(self):
        if hasattr(self._parent, '_mark_as_changed'):
            self._parent._mark_as_changed(self._name, self)
    
    def _add_operation(self, op, key, val):
        if hasattr(self._parent, 'add_operation'):
            self._parent.add_operation(op, key, val)
