"""
Implementations of field type parsing, validation, and encoding.
"""

from __future__ import absolute_import

import copy
import datetime
import uuid
import bson
import bson.objectid

from .exceptions import ValidationError
from .ChangeTrackingDict import ChangeTrackingDict
from .ChangeTrackingList import ChangeTrackingList
from .ChangeTrackingSet import ChangeTrackingSet


def is_basic(*fields):
    """Return ``True`` if all :py:class:`Field` instances from `types` have
    default :py:meth:`Field.expand` and :py:meth:`Field.collapse`
    implementations. This permits avoiding a few expensive operations."""
    expect = (Field.collapse.im_func, Field.expand.im_func)
    try:
        for field in fields:
            if expect != (field.collapse.im_func, field.expand.im_func):
                return False
    except AttributeError:
        return False
    return True


def type_name(o):
    s = getattr(o, '__name__', None)
    if not s:
        s = o.__class__.__name__
    return s.split('.')[-1]


class Field(object):
    """Default field type, does no validation, accepts anything. Created by
    passing ``None`` as the field type:

        ::

            class Foo(Document):
                structure = {'anything': None}

    If the default field value is a function, it will be invoked each time a
    default is required. The default value must pass :py:meth:`validate`.

    Field instances are also descriptors: they implement ``__get__`` and
    ``__set__``, automatically invoking :py:meth:`expand` and
    :py:meth:`collapse` as necessary. For super special field types (like
    lists), you may directly override ``__get__`` and ``__set__`` to generate
    semantic operations rather than simple property overrides.
    """
    #: Default 'default' value used if user type specification does not include
    #: a default for the field. Used by :py:meth:`make`.
    _DEFAULT = None

    def __init__(self, name=None, required=False, default=None,
                 doc=None, readonly=False, write_once=False):
        """Create an instance.
        """
        self.name = name
        self.required = required
        if doc is not None:
            self.__doc__ = doc
        self.readonly = readonly
        self.write_once = write_once
        if default is None:
            default = self._DEFAULT
        if callable(default):
            # Shadow Field.make() using the user-provided callable.
            self.make = default
        else:
            self.default = default
        # TODO: commented out until None vs. _DEFAULT mess fixed.
        #self.validate(self.make())

    def __repr__(self):
        return '<%s name=%r>' % (self.__class__.__name__, self.name)

    def __get__(self, obj, klass):
        """Implement the descriptor protocol by fetching the collapsed
        attribute from the :py:class:`Document` if it exists, expanding and
        returning it, otherwise return the default value if one is set,
        otherwise ``None``.
        """
        if obj is None:
            return self
        value = obj.get(self.name)
        if value is None:
            return self.make()
        else:
            return self.expand(value)

    def __set__(self, obj, value):
        """Implement the descriptor protocol by validating and collapsing the
        expanded `value` and saving it to the associated key of `obj`.
        """
        if self.readonly:
            raise ValidationError('%r is read-only' % (self.name,))
        if self.write_once and self.name in obj:
            raise ValidationError('%r is write-once' % (self.name,))
        if value is None:
            return obj.unset(self.name)
        self.validate(value)
        obj.set(self.name, self.collapse(value))

    def validate(self, value):
        """Raise an exception if `value` is not a suitable value for this
        field. The base implementation does nothing."""

    def collapse(self, value):
        """Transform `value` into a MongoDB-compatible form that will be
        persisted in the underlying document. Called after validation. The
        default implementation simply returns the original object."""
        return value

    def expand(self, value):
        """Transform `value` from a MongoDB-compatible form that was persisted
        in the underlying document, into the user-visible form. Called before
        validation. The default implementation simply returns the original
        object."""
        return value

    @classmethod
    def parse(cls, obj, **kwargs):
        """Attempt to parse `obj` as a type description defined in the
        Mongotron mini-language. If `obj` is unrecognizable, return ``None``,
        otherwise return a :py:class:`Field` instance describing it.

            `obj`:
                The mini-language object to attempt to parse.

            `kwargs`:
                Extra keyword arguments passed to Field's constructor on
                success.
        """
        if obj is None: # "None" means any value.
            return cls(**kwargs)

    def make(self):
        """Produce a default value for this field."""
        return copy.deepcopy(self.default)


class ListField(Field):
    """List validator. Ensure value type is a list, and that each element
    validates using the given element field type. Created by either referencing
    the ``list`` type, or an empty list:

        ::

            class Foo(Document):
                structure = {'list_one': list,  # Equivalent
                             'list_two': []}    # Equivalent

    Or by specifing a size-1 list whose element represents a specific type:

        ::

            class Foo(Document):
                structure = {'int_list': [int]}
    """
    _DEFAULT = []
    CONTAINER_TYPE = list
    EMPTY_VALUE = set()

    def __init__(self, element_type, **kwargs):
        """See Field.__init__()."""
        self.element_type = parse(element_type)
        self.basic = is_basic(element_type)
        Field.__init__(self, **kwargs)

    def wrap(self, value, obj):
        """Return a wrapped version of `value`, that somehow tracks changes to
        the container."""
        return ChangeTrackingList(value, obj, self.name)

    def __get__(self, obj, klass):
        """See Field.__get__. Returns a :py:class:`ChangeTrackingList` that
        generates semantic actions based on user modifications."""
        if obj is None:
            return self
        value = Field.__get__(self, obj, klass)
        return self.wrap(value or self.make(), obj)

    def validate(self, value):
        """See Field.validate()."""
        if not isinstance(value, self.CONTAINER_TYPE):
            raise ValidationError('value must be a %r, got %r.' %\
                                  (type_name(self.CONTAINER_TYPE), value))
        for elem in value:
            self.element_type.validate(elem)

    def collapse(self, value):
        """See Field.collapse(). Collapse each element in turn."""
        if self.basic:
            return value
        return map(self.element_type.collapse, value)

    def expand(self, value):
        """See Field.expand(). Expand each element in turn."""
        if self.basic:
            return value
        return map(self.element_type.expand, value)

    @classmethod
    def parse(cls, obj, **kwargs):
        """See Field.parse()."""
        # len(obj) > 1 is handled by FixedListField.
        if type(obj) is cls.CONTAINER_TYPE and len(obj) < 2:
            if len(obj) == 0:
                # Container of any value.
                element_type = Field()
            else:
                # Container with specific value type.
                element_type = parse(obj[0])
            return cls(element_type, **kwargs)
        elif obj == cls.EMPTY_VALUE or obj is cls.CONTAINER_TYPE:
            # structure = {'foo': []} or {'foo': list}
            element_type = Field()
            return cls(element_type, **kwargs)


class SetField(ListField):
    """Like ListField, except require set() instances instead. Created either
    by referencing the ``set`` type:

        ::

            class Foo(Document):
                structure = {'set_fld': set}

    Or by specifing a size-1 set whose element represents a specific type:

        ::

            class Foo(Document):
                structure = {'set_fld': set([int])}
    """
    _DEFAULT = set()
    CONTAINER_TYPE = set
    EMPTY_VALUE = set()

    def wrap(self, value, obj):
        """See ListField.wrap()"""
        return ChangeTrackingSet(value, obj, self)

    def collapse(self, value):
        """See Field.collapse(). Collapse each element and return a list."""
        return map(self.element_type.collapse, value)

    def expand(self, value):
        """See Field.expand(). Expan each element and return a set."""
        return set(self.element_type.expand(elem) for elem in value)


class FixedListField(Field):
    """A specifically sized list containing specifically typed elements.

    Created by specifying a list of more than one element, where each element
    is a reference to some other type:

        ::

            class Foo(Document):
                structure = {'fixed': [bool, unicode, float]}

            foo = Foo()
            foo.fixed = [True, u'Hello', 1.0]   # OK
            foo.fixed = [2, 0, 'Hello', 1]      # Error!
    """
    def __init__(self, element_types, default=None, **kwargs):
        """See Field.__init__()."""
        self.element_types = map(parse, element_types)
        self.basic = is_basic(*element_types)
        if default is None:
            default = [fld.make() for fld in self.element_types]
        Field.__init__(self, default=default, **kwargs)

    def wrap(self, value, obj):
        """Return a wrapped version of `value`, that somehow tracks changes to
        the container."""
        return ChangeTrackingList(value, obj, self.name)

    #: Borrow ListField's __get__ method.
    __get__ = ListField.__get__.im_func

    def validate(self, value):
        """See Field.validate()."""
        if type(value) is not list:
            raise ValidationError('value must be a %r.' %\
                                 (type_name(list),))
        expect_len = len(self.element_types)
        if len(value) != expect_len:
            raise ValidationError('value must contain %d elements.' %\
                                  expect_len)
        for idx, elem in enumerate(value):
            self.element_types[idx].validate(elem)

    def collapse(self, value):
        """See Field.collapse()."""
        if self.basic:
            return value
        return [self.element_types[i].collapse(elem)
                for i, elem in enumerate(value)]

    def expand(self, value):
        """See Field.expand()."""
        if self.basic:
            return value
        return [self.element_types[i].expand(elem)
                for i, elem in enumerate(value)]

    @classmethod
    def parse(cls, obj, **kwargs):
        """See Field.parse()."""
        if type(obj) is list and len(obj) > 1:
            return cls(element_types=obj, **kwargs)


class DictField(Field):
    """Field containing a dict value. Ensures value type is a dict, and that
    each element validates using the given element key and value type. Created
    either by referencing the ``dict`` type:

        ::

            class Foo(Document):
                structure = {'dict_field': dict}

    Or by specifing a size-1 dict whose key and value represent specific types:

        ::

            class Foo(Document):
                structure = {'dict_field': {int: str}}
    """
    _DEFAULT = {}

    def __init__(self, key_type, value_type, **kwargs):
        """See Field.__init__()."""
        self.key_type = parse(key_type)
        self.value_type = parse(value_type)
        self.basic = is_basic(self.key_type, self.value_type)
        Field.__init__(self, **kwargs)

    def __get__(self, obj, klass):
        """See Field.__get__. Returns a :py:class:`ChangeTrackingList` that
        generates semantic actions based on user modifications."""
        if obj is None:
            return self
        value = Field.__get__(self, obj, klass)
        return ChangeTrackingDict(value or self.make(), obj, self.name)

    def validate(self, dct):
        """See Field.validate()."""
        if not isinstance(dct, dict):
            raise ValidationError('value must be a %r.' % (type_name(dict),))
        for key, value in dct.iteritems():
            self.key_type.validate(key)
            self.value_type.validate(value)

    def collapse(self, dct):
        """See Field.collapse(). Collapse each element in turn."""
        if self.basic:
            return dct
        return dict((self.key_type.collapse(k), self.value_type.collapse(v))
                    for k, v in dct.iteritems())

    def expand(self, value):
        """See Field.expand(). Expand each element in turn."""
        if self.basic:
            return value
        return dict((self.key_type.expand(k), self.value_type.expand(v))
                    for k, v in dct.iteritems())

    @classmethod
    def parse(cls, obj, **kwargs):
        """See Field.parse()."""
        if type(obj) is dict:
            if len(obj) == 0:
                return cls(None, None, **kwargs)
            elif len(obj) == 1:
                return cls(*next(obj.iteritems()), **kwargs)
        elif obj == {} or obj is dict:
            # structure = {'foo': {}} or {'foo': dict}
            return cls(None, None, **kwargs)


class ScalarField(Field):
    """A scalar field that must contain a specific set of types.
    """
    def validate(self, value):
        if not isinstance(value, self._TYPES):
            allowed = ' or '.join(t.__name__ for t in self._TYPES)
            actual = '%s (%r)' % (type(value).__name__, value)
            raise ValidationError('%s: value must %s, not %s' %\
                                  (self.name, allowed, actual))

    @classmethod
    def parse(cls, obj, **kwargs):
        """See Field.parse()."""
        if obj in cls._TYPES:
            return cls(**kwargs)


class UuidField(ScalarField):
    _TYPES = (uuid.UUID,)



class BoolField(ScalarField):
    """A boolean value. Created by referencing
    ``bool``:

        ::

            class Foo(Document):
                structure = {'bool_field': bool}
    """
    _TYPES = (bool,)
    _DEFAULT = False


class BlobField(ScalarField):
    """A BLOB value. Created by referencing the ``str`` or ``bytes``
    types:

        ::

            class Foo(Document):
                structure = {'blob_field': str}
    """
    _TYPES = (bytes,) # Alias of str() in Python 2.x
    _DEFAULT = b''

    def collapse(self, value):
        """See Field.collapse(). Wrap the bytestring in a bson.Binary()
        instance."""
        return bson.Binary(value)

    def expand(self, value):
        """See Field.expand(). Unwrap the bson.Binary() instance into a
        bytestring."""
        if not isinstance(value, (bson.Binary, str)):
            raise ValidationError('%r must be a BLOB, got %r' %\
                                  (self.name, value))
        return str(value)


class TextField(ScalarField):
    """A Unicode value. Created by referencing the ``unicode``
    type:

        ::

            class Foo(Document):
                structure = {'text_field': unicode}
    """
    # TODO: this is completely totally and utterly wrong, find a better
    # solution later.
    _TYPES = (basestring, unicode)
    _DEFAULT = u''


class DatetimeField(ScalarField):
    """A field containing a Python datetime.datetime value. Created by
    referencing the ``datetime.datetime`` type:

        ::

            import datetime

            class Foo(Document):
                structure = {'time': datetime.datetime}
    """
    _DEFAULT = None
    _TYPES = (datetime.datetime,)


class ObjectIdField(ScalarField):
    """A field that must contain a BSON ObjectID. Created by referencing the
    ``bson.ObjectId`` type:

        ::

            import bson

            class Foo(Document):
                structure = {'oid_field': bson.ObjectId}
    """
    _DEFAULT = None
    _TYPES = (bson.objectid.ObjectId, type(None))


class IntField(ScalarField):
    """A field that must contain an int or long. The value is always coerced to
    a MongoDB NumberLong, and returned as a long. Created by referencing the
    ``int`` or ``long`` types. The following are equivalent:

        ::

            class Foo(Document):
                structure = {'first_int': int,
                             'second_int': long}
    """
    _DEFAULT = 0
    _TYPES = (int, long)

    #: See Field.collapse(). Unconditionally force all values to be
    #: longs.
    collapse = staticmethod(long)


class FloatField(ScalarField):
    """A field that must containing a float. Created by referencing the
    ``float`` type:

        ::

            class Foo(Document):
                structure = {'some_float': flaot}
    """
    _DEFAULT = 0.0
    _TYPES = (float,)


class DocumentField(Field):
    """A field containing a sub-document. Created by referencing the
    :py:class:`Document <mongotron.Document>` subclass directly:

        ::

            class Foo(Document):
                structure = {'subdoc': SubDocumentClass}
    """
    def __init__(self, doc_type, default=None, **kwargs):
        """See Field.__init__()."""
        if default is None:
            default = doc_type()
        self.doc_type = doc_type
        self._TYPES = (doc_type,)
        Field.__init__(self, default=default, **kwargs)

    def validate(self, value):
        """See Field.validate(). Adds type checking and sub-document
        validation."""
        if not isinstance(value, self.doc_type):
            raise ValidationError('value must be a %r.' %\
                                  (type_name(self.doc_type),))
        value.validate()

    def collapse(self, value):
        """Return the document value as a dictionary."""
        return value.document_as_dict()

    def expand(self, value):
        """Produce a Document instance from the dict `value`."""
        if not isinstance(value, dict):
            raise ValidationError('%r must be a dict, got %r' %\
                                  (self.name, value))
        return self.doc_type(doc=value)

    @classmethod
    def parse(cls, obj, **kwargs):
        """See Field.parse()."""
        # TODO: cyclical imports bad design
        from mongotron.Document import Document
        if issubclass(obj, Document):
            return cls(obj, **kwargs)


#: List of Field classes in the order in which parsing should be attempted.
#: Currently parsing is unambiguous, but this might not always be true.
TYPE_ORDER = [
    Field,
    ListField,
    SetField,
    FixedListField,
    DictField,
    BoolField,
    BlobField,
    TextField,
    DatetimeField,
    IntField,
    FloatField,
    ObjectIdField,
    DocumentField,
    UuidField
]


def parse(obj, **kwargs):
    """Given some mini-language description of a field type, return a
    :py:class:`Field` instance describing that type.

        `obj`:
            The mini-language object to attempt to parse.

        `kwargs`:
            Extra keyword arguments passed to Field's constructor on
            success.
    """
    if isinstance(obj, Field):
        # User provided a fully-formed Field instance.
        return obj

    for klass in TYPE_ORDER:
        field = klass.parse(obj, **kwargs)
        if field:
            return field
    raise ValueError('%r cannot be parsed as a field type.' % (obj,))
