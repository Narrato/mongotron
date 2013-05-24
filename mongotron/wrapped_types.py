
import functools


def make_wrapper(func):
    def wrapper(self, *args, **kwargs):
        ret = func(*args, **kwargs)
        self._mark_as_changed()
        return ret

    wrapper.func_name = func.__name__
    return wrapper


def wrap_type(name, base, mutators):
    """Given some built-in collection type, wrap all its mutator methods in a
    subclass such that its owner document is notification when the collection
    is modified.
    """
    def _set(self):
        """Arrange for the parent to notice the container has changed."""
        self._parent.set(self._field.name, self._field.collapse(self))

    def __getstate__(self):
        """Prevent pickling without explicit conversion."""
        raise TypeError(name + ' cannot be pickled')

    type_ = None
    def __init__(self, items, parent, field):
        self._parent = parent
        self._field = field
        super(type_, self).__init__(items)

    dct = {
        '__init__': __init__,
        '_set': _set,
        '__getstate__': __getstate__
    }
    dct.update((name, make_wrapper(getattr(base, name)))
               for name in mutators)
    type_ = type(name, (base,), dct)
    return type_


ChangeTrackingDict = wrap_type('ChangeTrackingDict', dict, mutators=[
    '__setitem__', '__delitem__', 'clear', 'pop', 'popitem', 'update'
])

ChangeTrackingList = wrap_type('ChangeTrackingList', list, mutators=[
    '__setitem__', '__delitem__', 'append', 'extend', 'insert', 'pop',
    'remove', 'reverse', 'sort'
])

ChangeTrackingSet = wrap_type('ChangeTrackingSet', set, mutators=[
    'clear', 'pop', '__iand__', '__ior__', '__isub__', '__ixor__', 'add',
    'difference_update', 'discard', 'intersection_update', 'remove',
    'symmetric_difference_update', 'update'
])
