"""
Implementations of field type parsing, validation, and encoding.
"""

import copy


#: Represent an undefined argument, since None is a valid field value.
UNDEFINED = object()


class Field(object):
    """Default field type, does no validation, accepts anything.
    """
    #: Default 'default' value used if user type specification does not include
    #: a default for the field. Used by :py:meth:`make`.
    DEFAULT_DEFAULT = None

    def __init__(self, required=False, default=UNDEFINED):
        """Create an instance."""
        self.required = required
        if default is UNDEFINED:
            default = self.DEFAULT_DEFAULT
        self.default = default
        self.validate(self.default)

    def validate(self, value):
        """Do nothing."""

    @classmethod
    def parse(cls, obj):
        """Attempt to parse `obj` as a type description defined in the
        Mongotron mini-language. If `obj` is unrecognizable, return ``None``,
        otherwise return a :py:class:`Field` instance describing it.
        """
        if obj is None: # "None" means any value.
            return cls()

    def make(self):
        """Produce a default value for this field."""
        return copy.copy(self.default)


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

    @classmethod
    def parse(cls, obj):
        """See Field.parse()."""
        if type(obj) is self.CONTAINER_TYPE:
            # Container of any value.
            if len(obj) == 0:
                return cls(element_type=Field())
            elif len(obj) == 1:
                # Container with specific value type.
                return cls(element_type=parse(obj[0]))
            # len(obj) > 1 is handled by FixedListField.


class SetField(Field):
    """Like ListField, except require set() instances instead.
    """
    DEFAULT_DEFAULT = set()
    CONTAINER_TYPE = set


class FixedListField(Field):
    """A specifically sized list containing specifically typed elements.
    """
    def __init__(self, element_types, required=False, default=UNDEFINED):
        """See Field.__init__()."""
        assert all(isinstance(Field, e) for e in element_types)
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
        if obj in self.TYPES:
            return cls(required=required, default=default)


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


#: List of Field classes in the order in which parsing should be attempted.
#: Currently parsing is unambiguous, but this might not always be true.
TYPE_ORDER = [Field, ListField, SetField, FixedListField, IntField, FloatField]

def parse(obj, required=False, default=MISSING):
    """Given some mini-language description of a field type, return a Field
    instance describing that type.
    """
    for klass in TYPE_ORDER:
        field = klass.parse(obj, required, default)
        if field:
            return field
    raise ValueError('%r cannot be parsed as a field type.' % (obj,))
