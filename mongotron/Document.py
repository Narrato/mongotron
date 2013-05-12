
from __future__ import absolute_import

import logging
from collections import OrderedDict

from bson.objectid import ObjectId, InvalidId

from .exceptions import ValidationError
from .ConnectionManager import GetConnectionManager
from .Cursor import Cursor
from . import field_types

LOG = logging.getLogger('mongotron.Document')


class classproperty(object):
    """Equivalent to property() on a class, i.e. invoking the descriptor
    results in the wrapped function being invoked and its return value being
    used as the descriptor's value.
    """
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class DocumentMeta(type):
    """This is the metaclass for :py:class:`Document`; it is responsible for
    merging :py:attr:`Document.structure`, :py:attr:`Document.field_map` and
    :py:attr:`Document.default_values` with any base classes.

    After this is done, it synthesizes a new :py:attr:`Document.field_types`
    mapping using :py:class:`mongotron.field_types.Field` Field instances.
    """
    INHERITED_DICTS = ['structure', 'default_values', 'field_map']
    INHERITED_SETS = ['required', 'write_once']

    def __new__(cls, name, bases, attrs):
        for base in bases:
            parent = base.__mro__[0]
            for dname in cls.INHERITED_DICTS:
                cls.merge_carefully(parent, dname, attrs)

        for sname in cls.INHERITED_SETS:
            val = set()
            val.update(attrs.get(sname, []))
            for base in bases:
                val.update(vars(base.__mro__[0]).get(sname, []))
            attrs[sname] = val

        it = attrs['field_map'].iteritems()
        attrs['inverse_field_map'] = dict((k, v) for k, v in it)
        attrs['field_types'] = cls.make_field_types(attrs)
        attrs['__collection__'] = cls.make_collection_name(name, attrs)
        attrs.setdefault('__manager__', GetConnectionManager())
        attrs.setdefault('__connection__', None)
        # Fields are descriptors for their corresponding attribute.
        attrs.update(attrs['field_types'])

        # print '----------------------------------------'
        # pprint(attrs)
        # print '----------------------------------------'
        return type.__new__(cls, name, bases, attrs)

    @classmethod
    def merge_carefully(cls, base, dname, attrs):
        """Merge the contents a base class attribute's dictionary into the
        child, complaining bitterly if a duplicate entry exists."""
        base_dct = getattr(base, dname, {})
        dct = attrs.setdefault(dname, {})
        assert all(type(d) is dict for d in (base_dct, dct))
        for key, value in base_dct.iteritems():
            if key in dct and key != '_id':
                raise TypeError('duplicate %r of dict %r appears in base %r' %\
                                (key, dname, base))
            dct[key] = value

    @classmethod
    def make_collection_name(cls, name, attrs):
        """Form a collection name for the class, or use the user-provided
        name."""
        return str(attrs.get('__collection__', name.lower()))

    @classmethod
    def make_field_types(cls, attrs):
        """Return a mapping of field names to :py:class:`Field` instances
        describing that field.
        """
        types = {}
        for name, desc in attrs['structure'].iteritems():
            types[name] = field_types.parse(desc,
                required=name in attrs['required'],
                default=attrs['default_values'].get(name),
                name=name,
                write_once=name in attrs['write_once'])
        return types


class Document(object):
    """A class with property-style access. It maps attribute access to an
    internal dictionary, and tracks changes.
    """
    __metaclass__ = DocumentMeta
    __should_explain = False

    #: Map of canonical field names to objects representing the required type
    #: for that field.
    structure = {
        '_id': field_types.ObjectIdField(name='_id',
            doc="""The underlying document's _id field, or ``None`` if the
                   document has never been saved.""", readonly=True)
    }

    #: List of canonical field names that absolutely must be set prior to save.
    #: Automatically populated by metaclass.
    required = []

    #: List of canonical field names that cannot be overwritten once they have
    #: been set.
    write_once = []

    #: Map of canonical field names to their default values.
    default_values = {}

    #: Map of canonical field names to shortened field names. Automatically
    #: populated by metaclass.
    field_map = {}

    #: Map of shortened field names to canonical field names. Automatically
    #: populated by metaclass.
    inverse_field_map = {}

    #: Map of canonical field names to Field instances describing the field.
    #: Automatically populated by metaclass.
    field_types = {}

    def validate(self):
        """Hook invoked prior to creating or updating document, but after
        :py:meth:`pre_save`, :py:meth:`pre_update` or :py:meth:`pre_insert`
        have run. Expected to raise an exception if the document's structure
        does not make sense.

        The base implementation must be called in order to handle
        :py:attr:`Document.required` processing.
        """
        missing = self.required.difference(self.__attributes)
        if missing:
            raise ValidationError('missing required fields: %s' %\
                                  (', '.join(missing),))
                                  
    def on_load(self):
        """Hook invoked while document is being initialized.
        :py:meth:`on_load` is only called if the Document is being loaded
        from mongo (i.e. is it being initialized with Data) Override in your
        subclass as desired, but remember to call super()
        """

    def pre_save(self):
        """Hook invoked prior to creating or updating a document.
        :py:meth:`pre_save` is always invoked before :py:meth:`pre_insert` or
        :py:meth:`pre_update`. Any mutations produced by :py:meth:`pre_save`
        will be reflected in the saved document. Override in your subclass as
        desired."""

    def post_save(self):
        """Hook invoked after a document has been created or updated
        successfully. :py:meth:`pre_save` is always invoked before
        :py:meth:`pre_insert` or :py:meth:`pre_update`. Override in your
        subclass as desired."""

    def pre_insert(self):
        """Hook invoked prior to document creation, but after
        :py:meth:`pre_save`. Any mutations produced by :py:meth:`pre_insert`
        will be reflected in the saved document. Override in your subclass as
        desired."""

    def post_insert(self):
        """Hook invoked after document creation, but after
        :py:meth:`post_save`. Override in your subclass as desired."""

    def pre_update(self):
        """Hook invoked prior to document update, but after
        :py:meth:`pre_save`. Any mutations produced by :py:meth:`pre_update`
        will be reflected in the saved document. Override in your subclass as
        desired."""

    def post_update(self):
        """Hook invoked after document update, but after :py:meth:`post_save`.
        Override in your subclass as desired."""

    @classproperty
    def _dbcollection(cls):
        conn = cls.__manager__.get_connection(cls.__connection__, True)
        try:
            return conn[cls.__db__][cls.__collection__]
        except AttributeError:
            raise AttributeError('__db__ field is not set on your object!')

    @classmethod
    def long_to_short(cls, long_key):
        """Return the shortened field name for `long_key`, returning `long_key`
        if no short version exists."""
        return cls.field_map.get(long_key, long_key)

    @classmethod
    def short_to_long(cls, short_key):
        """Return the canonical field name for `short_key`, returning
        `short_key` if no canonical version exists."""
        return cls.inverse_field_map.get(short_key, short_key)

    def merge_dict(self, dct):
        """Load keys and collapsed values from `dct`.
        """
        for key, field in self.field_types.iteritems():
            short = self.long_to_short(key)
            if short in dct:
                self.__attributes[key] = dct[short]
            elif key in self.default_values:
                self.set(key, field.make())

    def load_dict(self, dct):
        """Reset the document to an empty state, then load keys and values from
        the dictionary `doc`."""
        self.clear_ops()
        self.__attributes = {}
        self.merge_dict(dct)
        self.__identity = self.identity()

    def to_json_dict(self, **kwargs):
        return OrderedDict()

    def from_json_dict(self, json_dict):
        pass

    def __init__(self, doc=None):
        self.load_dict(doc or {})
        if doc:
            self.on_load()

    def __setattr__(self, name, value):
        """Nasty guard to prevent object writes for nonexistent attributes. It
        should be possible to replace this with the ``__slots__`` mechanism,
        but there is some apparent incompatibility with using metaclasses and
        weakrefs simultaneously."""
        if name.startswith('_Document__'):
            vars(self)[name] = value
        else:
            getattr(self.__class__, name).__set__(self, value)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.__attributes)

    def __contains__(self, key):
        return key in self.__attributes

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self._id and other._id and
                self._id == other._id)

    def __hash__(self):
        return hash(self._id)

    @property
    def embedded(self):
        """For future embedded docs, will stop save() from working and will let
        the parent know it can be converted into a dict!"""
        return self._embedded

    def get(self, key):
        """Fetch the value of `key` from the underlying document, returning
        ``None`` if the value does not exist."""
        return self.__attributes.get(key)

    # mongo operation wrappers!
    def add_operation(self, op, key, val):
        """Arrange for the Mongo operation `op` to be applied to the document
        property `key` with the operand `value` during save.
        """
        if op != '$unset':
            try:
                val = self.field_types[key].collapse(val)
            except KeyError:
                raise KeyError('%r is not a settable key' % (key,))

        # We should probably make this smarter so you can't set a top level
        # array and a component at the same time though if you're doing that,
        # your code is broken anyway
        key = self.long_to_short(key)

        op_dict = self.__ops.setdefault(op, {})
        if op == '$set':
            raise ValueError('$set is an invalid operation!')
        elif op == '$addToSet':
            # $addToSet gets special handling because we use the $each version
            if not key in op_dict:
                op_dict[key] = { '$each':[] }
            param_dict = op_dict[key]
            param_list = param_dict['$each']

            if isinstance(val, list):
                param_list.extend(val)
            else:
                param_list.append(val)
        else:
            param_list = op_dict.setdefault(key, [])
            if isinstance(val, list):
                param_list.extend(val)
            else:
                param_list.append(val)

    @property
    def operations(self):
        # construct the $set changes
        ops = self.__ops.copy()
        ops['$set'] = {self.long_to_short(key): self.__attributes[key]
                       for key in self.__dirty_fields}
        return ops


    def clear_ops(self):
        """Reset the list of field changes tracked on this document. Note this
        will not reset field values to their original.
        """
        self.__ops = {}
        self.__dirty_fields = set()


    # MONGO MAGIC HAPPENS HERE!

    def mark_dirty(self, key):
        """Explicitly mark the field `key` as modified, so that it will be
        mutated during :py:meth:`save`."""
        #TODO: add this key to the $set listd
        self.__dirty_fields.add(key)

    def set(self, key, value):
        """Unconditionally set the underlying document field `key` to `value`.
        If `value` is ``None``, then behave as if :py:meth:`unset` was invoked.

        Note this does no type checking or field name mapping; you may use it
        to directly modify the underlying Mongo document.
        """
        if value is None:
            return self.unset(key)
        self.__dirty_fields.add(key)
        self.__attributes[key] = value

    def unset(self, key):
        """Unconditionally remove the underlying document field `key`.

        Note this does no type checking or field name mapping; you may use it
        to directly modify the underlying Mongo document.

        This operation can also be unvoked using `del`:

        ::

            >>> # Equivalent to instance.unset('attr'):
            >>> del instance.attr
        """
        self.__attributes.pop(key, None)
        self.add_operation('$unset', key, 1)

    __delattr__ = unset

    def inc(self, key, value=1):
        """Increment the value of `key` by `value`.
        """
        self.add_operation('$inc', key, value)

    def dec(self, key, value=1):
        """Decrement the value of `key` by `value`.
        """
        self.add_operation('$inc', key, -abs(value))

    #addToSet gets special handling because we use the $each version
    def addToSet(self, key, value):
        #this is a bit more complicated
        #what we need to do is store an "each" part
        self.add_operation('$addToSet', key, value)


    # we translate push and pull into pushAll and pullAll
    # so that we can queue up the operations!
    def pull(self, key, value):
        #this is a bit more complicated
        #what we need to do is store an "each" part
        self.add_operation('$pullAll', key, value)


    def push(self, key, value):
        """Append `value` to the list-valued `key`.
        """
        self.add_operation('$pushAll', key, value)

    def identity(self):
        """Return a MongoDB query that matches this document. Needed to
        correctly form findAndModify command in a sharded environment (i.e. you
        must override this and have it return a query that matches all fields
        in the assocated collection's shardkey.

        Base implementation simply returns _id or a new ObjectId.
        """
        return {'_id': self._id or ObjectId()}

    def save(self, safe=True):
        """Insert the document into the underlying collection if it is unsaved,
        otherwise update the existing document.

            `safe`:
                Does nothing, yet.
        """
        self.pre_save()

        # NOTE: called BEFORE we get self.operations to allow the pre_
        # functions to add to the set of operations. (i.e. set last modified
        # fields etc)
        new = self._id is None
        if new:
            self.pre_insert()
        else:
            self.pre_update()

        self.validate()
        col = self._dbcollection
        ops = self.operations

        if ops:
            res = col.find_and_modify(query=self.__identity,
                                      update=ops, upsert=True, new=True)
            self.load_dict(res)

        if new:
            self.post_insert()
        else:
            self.post_update()

        self.clear_ops()
        self.post_save()

    def delete(self):
        """Delete the underlying document. Returns ``True`` if the document was
        deleted, otherwise ``False`` if it did not exist.
        """
        # TODO: parse returned ack dict to ensure a deletion occurred.
        assert self._id, 'Cannot delete unsaved Document'
        self._dbcollection.remove({'_id':self._id})
        return True

    @classmethod
    def map_search_list(cls, search_list):
        newlist = []
        for v in search_list:
            if isinstance(v, dict):
                v = cls.map_search_dict(v)
            elif isinstance(v,list):
                v = cls.map_search_list(v)
            newlist.append(v)

        return newlist

    @classmethod
    def map_search_dict(cls, search_dict):
        newdict = {}
        for k in search_dict:
            v = search_dict[k]

            if isinstance(v, dict):
                v = cls.map_search_dict(v)
            elif isinstance(v, list):
                v = cls.map_search_list(v)

            k = cls.long_to_short(k)
            newdict[k] = v

        return newdict

    @classmethod
    def find(cls, *args, **kwargs):
        """Like :py:meth:`Collection.find <pymongo.collection.Collection.find>`
        """
        if 'spec' in kwargs:
            kwargs['spec'] = cls.map_search_dict(kwargs['spec'])

        args = list(args)
        if len(args):
            args[0] = cls.map_search_dict(args[0])

        if 'slave_okay' not in kwargs and hasattr(cls._dbcollection, 'slave_okay'):
            kwargs['slave_okay'] = cls._dbcollection.slave_okay
        if 'read_preference' not in kwargs and hasattr(cls._dbcollection, 'read_preference'):
            kwargs['read_preference'] = cls._dbcollection.read_preference
        if 'tag_sets' not in kwargs and hasattr(cls._dbcollection, 'tag_sets'):
            kwargs['tag_sets'] = cls._dbcollection.tag_sets
        if 'secondary_acceptable_latency_ms' not in kwargs and \
                hasattr(cls._dbcollection, 'secondary_acceptable_latency_ms'):
            kwargs['secondary_acceptable_latency_ms'] = (
                cls._dbcollection.secondary_acceptable_latency_ms
            )

        return Cursor(cls._dbcollection, document_class=cls, *args, **kwargs)

    @classmethod
    def find_one(cls, spec_or_id=None, *args, **kwargs):
        """Find a document with the given ObjectID `spec_or_id`. You can pass
        an ObjectId in and it'll auto-search on the _id field.

        Like :py:meth:`Collection.find_one <pymongo.collection.Collection.find_one>`
        """
        if 'spec' in kwargs:
            kwargs['spec'] = cls.map_search_dict(kwargs['spec'])

        args = list(args)
        if len(args):
            args[0] = cls.map_search_dict(args[0])

        collection = cls._dbcollection
        #thing = collection.find_one(*args,**kwargs)

        if spec_or_id is not None and not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}

        for result in cls.find(spec_or_id, *args, **kwargs).limit(-1):
            return result
        return None

    @classmethod
    def update(cls, spec, update, **kwargs):
        """Modify existing documents matching `spec` using the operations from
        `update`.

        Like :py:meth:`Collection.update <pymongo.collection.Collection.update>`
        """
        #TODO: implement update
        return cls._dbcollection.update(spec, document, **kwargs)

    @classmethod
    def get_by_id(cls, oid):
        """
        Get a document by a specific ID. This is mapped to the _id field. You
        can pass a string or an ObjectId.
        """
        # Convert id to ObjectId
        if isinstance(oid, basestring):
            try:
                oid = ObjectId(oid)
            except:
                return None
        elif not isinstance(oid, ObjectId):
            raise ValueError('oid should be an ObjectId or string')

        return cls.find_one({'_id':oid})

    def document_as_dict(self):
        """Return a dict representation of the document suitable for encoding
        as BSON."""
        return {self.long_to_short(key): val
                for key, val in self.__attributes.iteritems()}
