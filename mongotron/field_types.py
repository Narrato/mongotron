"""
Implementations of field type parsing, validation, and encoding.
"""

import copy
import bson
import bson.objectid


#: Represent an undefined argument, since None is a valid field value.
UNDEFINED = object()


class Field(object):
    """Default field type, does no validation, accepts anything.

    If the default field value is a function, it will be invoked each time a
    default is required. The default value must pass :py:meth:`validate`.
    """
    #: Default 'default' value used if user type specification does not include
    #: a default for the field. Used by :py:meth:`make`.
    DEFAULT_DEFAULT = None

    def __init__(self, required=False, default=UNDEFINED):
        """Create an instance.
        """
        self.required = required
        if default is UNDEFINED:
            default = self.DEFAULT_DEFAULT
        if callable(default):
            # Shadow Field.make() using the user-provided callable.
            self.make = default
        else:
            self.default = default
        self.validate(self.make())

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
    def parse(cls, obj, required, default):
        """Attempt to parse `obj` as a type description defined in the
        Mongotron mini-language. If `obj` is unrecognizable, return ``None``,
        otherwise return a :py:class:`Field` instance describing it.
        """
        if obj is None: # "None" means any value.
            return cls(required=required, default=default)

    def make(self):
        """Produce a default value for this field."""
        return copy.deepcopy(self.default)


class ListField(Field):
    """List validator. Ensure value type is a list, and that each element
    validates using the given element field type.
    """
    DEFAULT_DEFAULT = []
    CONTAINER_TYPE = list

    def __init__(self, element_type, required=False, default=UNDEFINED):
        """See Field.__init__()."""
        assert isinstance(Field, element_type)
        self.element_type = element_type
        Field.__init__(self, required=required, default=default)

    def validate(self, value):
        """See Field.validate()."""
        if not isinstance(value, self.CONTAINER_TYPE):
            raise ValueError('value must be a %r.' % (self.CONTAINER_TYPE,))
        for elem in value:
            self.element_type.validate(elem)

    def collapse(self, value):
        """See Field.collapse(). Collapse each element in turn."""
        if self.element_type.collapse != Field.collapse:
            value = map(self.element_type.collapse, value)
        return value

    def expand(self, value):
        """See Field.expand(). Expand each element in turn."""
        if self.element_type.expand != Field.expand:
            value = map(self.element_type.expand, value)
        return value

    @classmethod
    def parse(cls, obj, required, default):
        """See Field.parse()."""
        # len(obj) > 1 is handled by FixedListField.
        if type(obj) is cls.CONTAINER_TYPE and len(obj) < 2:
            if len(obj) == 0:
                # Container of any value.
                element_type = Field()
            else:
                # Container with specific value type.
                element_type = parse(obj[0])
            return cls(element_type, required, default)


class SetField(ListField):
    """Like ListField, except require set() instances instead.
    """
    DEFAULT_DEFAULT = set()
    CONTAINER_TYPE = set

    def collapse(self, value):
        """See Field.collapse(). Collapse each element and return a list."""
        return map(self.element_type.collapse, value)

    def expand(self, value):
        """See Field.expand(). Expan each element and return a set."""
        return set(self.element_type.expand(elem) for elem in value)


class FixedListField(Field):
    """A specifically sized list containing specifically typed elements.
    """
    def __init__(self, element_types, required=False, default=UNDEFINED):
        """See Field.__init__()."""
        assert all(isinstance(Field, e) for e in element_types)
        self.element_types = element_types
        if default is UNDEFINED:
            default = [fld.make() for fld in element_types]
        Field.__init__(self, required=required, default=default)

    def validate(self, value):
        """See Field.validate()."""
        if type(value) is not list:
            raise ValueError('value must be a %r.' % (list,))
        expect_len = len(self.element_types)
        if len(value) != expect_len:
            raise ValueError('value must contain %d elements.' % expect_len)
        for idx, elem in enumerate(value):
            self.element_types[idx].validate(elem)

    def collapse(self, value):
        """See Field.collapse()."""
        return [self.element_types[i].collapse(elem)
                for i, elem in enumerate(value)]

    def expand(self, value):
        """See Field.expand()."""
        return [self.element_types[i].expand(elem)
                for i, elem in enumerate(value)]

    @classmethod
    def parse(cls, obj, required, default):
        """See Field.parse()."""
        if type(obj) is list and len(obj) > 1:
            return cls(element_types=map(parse, obj))


class ScalarField(Field):
    """A scalar field that must contain a specific set of types.
    """
    def validate(self, value):
        if not isinstance(value, self.TYPES):
            raise ValueError('value must be one of %r.' % (self.TYPES,))

    @classmethod
    def parse(cls, obj, required, default):
        """See Field.parse()."""
        if obj in cls.TYPES:
            return cls(required=required, default=default)


class BoolField(ScalarField):
    """A boolean value."""
    TYPES = (bool,)


class BlobField(ScalarField):
    """A blob (bytes) value."""
    TYPES = (bytes,) # Alias of str() in Python 2.x

    def collapse(self, value):
        """See Field.collapse(). Wrap the bytestring in a bson.Binary()
        instance."""
        return bson.Binary(value)

    def expand(self, value):
        """See Field.expand(). Unwrap the bson.Binary() instance into a
        bytestring."""
        return str(value)


class UnicodeField(ScalarField):
    """A unicode value."""
    TYPES = (unicode,)


class ObjectIdField(ScalarField):
    """A scalar field that must contain a BSON ObjectID.
    """
    DEFAULT_DEFAULT = None
    TYPES = (bson.objectid.ObjectId, type(None))


class IntField(ScalarField):
    """A scalar field that must contain an int or long.
    """
    DEFAULT_DEFAULT = 0
    TYPES = (int, long)


class FloatField(ScalarField):
    """A scalar field that must containing a float.
    """
    DEFAULT_DEFAULT = 0.0
    TYPES = (float,)


class DocumentField(Field):
    """A field containing a sub-document.
    """
    def __init__(self, doc_type, required=False, default=UNDEFINED):
        """See Field.__init__()."""
        if default is UNDEFINED:
            default = doc_type()
        self.doc_types = doc_type
        self.TYPES = (doc_type,)
        Field.__init__(self, required, default)

    def collapse(self, value):
        """Return the document value as a dictionary."""
        return value.document_as_dict()

    def expand(self, value):
        """Produce a Document instance from the dict `value`."""
        return self.doc_type(doc=value)

    @classmethod
    def parse(cls, obj, required, default):
        """See Field.parse()."""
        # TODO: cyclical imports bad design
        from mongotron.Document import Document
        if issubclass(obj, Document):
            return cls(obj, required=required, deafult=default)


#: List of Field classes in the order in which parsing should be attempted.
#: Currently parsing is unambiguous, but this might not always be true.
TYPE_ORDER = [
    Field,
    ListField,
    SetField,
    FixedListField,
    BoolField,
    BlobField,
    UnicodeField,
    IntField,
    FloatField,
    ObjectIdField,
    DocumentField
]


def parse(obj, required=False, default=UNDEFINED):
    """Given some mini-language description of a field type, return a Field
    instance describing that type.
    """
    for klass in TYPE_ORDER:
        field = klass.parse(obj, required, default)
        if field:
            return field
    raise ValueError('%r cannot be parsed as a field type.' % (obj,))