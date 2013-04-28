#from MongoEngine

from __future__ import absolute_import
import weakref

#TODO: implement this so it actually does the right thing I think

class ChangeTrackingSet(set):
    """A special set so we can watch any changes
    """

    _parent = None
    _field = None

    def __init__(self, items, parent, field):
        self._parent = parent
        self._field = field
        return super(ChangeTrackingSet, self).__init__(items)

    def _set(self):
        """Arrange for the parent object to notice the value of the set has
        changed."""
        self._parent.set(self._field.name, self._field.collapse(self))

    nullary_meths = ['clear', 'pop']
    unary_meths = ['__iand__', '__ior__', '__isub__', '__ixor__', 'add',
                   'difference_update', 'discard', 'intersection_update',
                   'remove', 'symmetric_difference_update', 'update']

    def _make_nullary(name, base):
        """Produce a function that wraps the 0-argument function `base`,
        invoking it before calling :py:meth:`_set`."""
        def meth(self):
            r = base(self)
            self._set()
            return r
        meth.func_name = name
        return meth

    def _make_unary(name, base):
        """Produce a function that wraps the 1-argument function `base`,
        invoking it before calling :py:meth:`_set`."""
        def meth(self, o):
            r = base(self, o)
            self._set()
            return r
        meth.func_name = name
        return meth

    for name in nullary_meths:
        locals()[name] = _make_nullary(name, getattr(set, name))
    for name in unary_meths:
        locals()[name] = _make_unary(name, getattr(set, name))

    del _make_nullary
    del _make_unary
    del name
