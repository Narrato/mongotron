from ConnectionManager import GetConnectionManager
from bson.objectid import ObjectId, InvalidId
from Cursor import Cursor

from ChangeTrackingList import ChangeTrackingList
from ChangeTrackingDict import ChangeTrackingDict

from collections import OrderedDict

from mongotron import field_types


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
    def __new__(cls, name, bases, attrs):
        field_types = {}
        required = set()

        for base in bases:
            parent = base.__mro__[0]
            for dname in 'structure', 'default_values', 'field_map':
                parent_dct = getattr(parent, dname, {})
                assert isinstance(parent_dct, dict)
                attrs[dname] = dict(parent_dct, **attrs.get(dname, {}))

            required.update(getattr(parent, 'required', []))

        attrs['required'] = list(required)
        attrs['field_map'] = cls.make_field_map(attrs)
        #print '----------------------------------------'
        #print attrs['structure']
        #print attrs['default_values']
        #print attrs['required']
        #print attrs['field_map']
        #print '----------------------------------------'
        pprint(attrs)

        return type.__new__(cls, name, bases, attrs)

    @classmethod
    def make_field_map(cls, attrs):
        """Return a mapping of field names to :py:class:`Field` instances
        describing that field.
        """
        field_map = {}
        for name, desc in attrs['structure'].iteritems():
            default = attrs['default_values'].get(name, field_types.UNDEFINED)
            required = name in attrs.get('required', [])
            field_map[name] = field_types.parse(desc, required, default)
        return field_map


class Document(object):
    """A class with property-style access. It maps attribute access to an
    internal dictionary, and tracks changes.
    """

    __metaclass__ = DocumentMeta
    __should_explain = False
    __internalfields = frozenset(['_Document__attributes',
                                  '_Document__ops',
                                  '_Document__operations',
                                  '_Document__dirty_fields'])

    #: Map of canonical field names to objects representing the required type
    #: for that field.
    structure = {
        '_id': ObjectId
    }

    #: List of canonical field names that absolutely must be set prior to save.
    #: Automatically populated by metaclass.
    required = []

    #: Map of canonical field names to their default values.
    default_values = {}

    #: Map of canonical field names to shortened field names. Automatically
    #: populated by metaclass.
    field_map = {}

    #: Map of shortened field names to canonical field names. Automatically
    #: populated by metaclass.
    inverse_field_map = {}

    def pre_save(self):
        """Hook invoked prior to creating or updating a document.
        :py:meth:`pre_save` is always invoked before :py:meth:`pre_insert` or
        :py:meth:`pre_update`. Override in your subclass as desired."""

    def post_save(self):
        """Hook invoked after a document has been created or updated
        successfully. :py:meth:`pre_save` is always invoked before
        :py:meth:`pre_insert` or :py:meth:`pre_update`. Override in your
        subclass as desired."""

    def pre_insert(self):
        """Hook invoked prior to document creation, but after
        :py:meth:`pre_save`. Override in your subclass as desired."""

    def post_insert(self):
        """Hook invoked after document creation, but after
        :py:meth:`post_save`. Override in your subclass as desired."""

    def pre_update(self):
        """Hook invoked prior to document update, but after
        :py:meth:`pre_save`. Override in your subclass as desired."""

    def post_update(self):
        """Hook invoked after document update, but after :py:meth:`post_save`.
        Override in your subclass as desired."""

    @classproperty
    def _dbcollection(cls):
        connectionname = getattr(cls, '__connection__', None)

        db = getattr(cls, '__db__', None)
        if db is None:
            raise StandardError('__db__ field is not set on your object!')

        collection = getattr(cls, '__collection__', None)
        if collection:
            col_name = str(collection)
        else:
            col_name = str(cls.__name__.lower())

        connection = GetConnectionManager().get_connection(connectionname, True)
        return connection[db][col_name]


    def key_in_structure(self, key):
        type_ = self.structure.get(key, None)
        if type_:
            return True
        raise AttributeError('%r has no field %r' % (self.__class__, key))

    def long_to_short(self, long_key):
        """Return the shortened field name for `long_key`, returning `long_key`
        if no short version exists."""
        return self.field_map.get(long_key, long_key)

    def short_to_long(self, short_key):
        """Return the canonical field name for `short_key`, returning
        `short_key` if no canonical version exists."""
        return self.inverse_field_map.get(short_key, short_key)

    def load_attr_dict(self, doc, default_keys=None):
        """Reset the document to an empty state, then load keys and values from
        the dictionary `doc`."""
        if default_keys is None:
            default_keys = set()
        self.__attributes = {}

        for key, value in doc.iteritems():
            k = self.short_to_long(k)
            self.field_map[k].validate(v)
            self.__attributes[k] = v
            default_keys.discard(k)

    def load_defaults_for_keys(self, default_keys):
        # any defaults that are left after loading the doc will be
        # set up now, if its callable it will be called
        for dk in default_keys:
            new_value = self.default_values[dk]

            if callable(new_value):
                new_value = new_value()
            elif isinstance(new_value, dict):
                new_value = deepcopy(new_value)
            elif isinstance(new_value, list):
                new_value = new_value[:]

            # we call set here so that its saved back to the db
            self.set(dk,new_value)

    def load_dict(self, dicttoload):
        default_keys = set(self.default_values.keys())
        self.load_attr_dict(dicttoload or {}, default_keys)
        self.load_defaults_for_keys(default_keys)


    def __init__(self, doc=None):
        self.__attributes = {}

        for k in self.field_map:
            v = self.field_map[k]
            self.inverse_field_map[v] = k

        self.clear_ops()
        self.load_dict(doc)


    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self.__attributes))

    def __contains__(self, key):
        return True if key in self.__attributes else False

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self._id and other._id and
                self._id == other._id)

    def __hash__(self):
        return hash(self._id)

    # for future embedded docs, will stop save() from working
    # and will let the parent know it can be converted into a dict!
    @property
    def embedded(self):
        return self._embedded

    @property
    def has_id(self):
        return '_id' in self.__attributes

    @property
    def _id(self):
        return self.__attributes['_id'] if '_id' in self.__attributes else None

    def get_attributes(self):
        return self.__attributes

    def set_attributes(self, attrs):
        self.__attributes = attrs

    def __getattr__(self, key):
        if key not in self.structure:
            return object.__getattribute__(self, key)

        #TODO: wrap lists & dictionaries in a mutation tracking version
        # which can push changes back up into our change set pls!
        value_type = self.structure[key]

        if key not in self.__attributes:
            #TODO: make this awesome please
            if isinstance(value_type, set):
                return set()
            if isinstance(value_type, list):
                return ChangeTrackingList([], self, key)
            if isinstance( value_type, dict ):
                return ChangeTrackingDict({}, self, key)
        else:
            value = self.__attributes[key]
            # If the structure was a set() then make damned sure we return a
            # set!
            if isinstance(value, list) and isinstance(value_type, set):
                return set(value)
            if isinstance(value, (list, tuple)) \
                    and not isinstance(value, ChangeTrackingList):
                return ChangeTrackingList(value, self, key)
            if isinstance(attr, dict) and not isinstance(attr, ChangeTrackingDict):
                return ChangeTrackingDict(attr, self, key)

    def __setattr__(self, key, value):
        if key in self.__internal_fields:
            return object.__setattr__(self, key, value)
        else:
            if key not in self.structure:
                raise ValueError('%s this is not a settable key' % key)
            self.field_types[key].validate(value)
            self.set(key, value)

    # mongo operation wrappers!
    def add_operation(self, op, key, val):
        """Arrange for the Mongo operation `op` to be applied to the document
        property `key` with the operand `value` during save.
        """
        if key not in self.structure:
            raise KeyError('%r is not a settable key' % (key,))

        # convert an assigned doc into a dict pls. we should also iterate over
        # lists doing the same thing :/ also what about converting the other
        # way?
        if isinstance(val, Document):
            val = val.document_as_dict()

        # TODO: get the value_type, if its an instance of a list. then if the
        # contents is a subclass of Document then transport that to a list of
        # dicts
        value_type = self.structure[key]

        # if we were passed a list, iterate it
        if isinstance(val,list):
            newval = []
            for passed_thing in val:
                if isinstance(passed_thing, Document):
                    newval.append(passed_thing.document_as_dict())
                else:
                    newval.append(passed_thing)
            val = newval

        # we should probably make this smarter so you can't set a top level
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
            if key not in op_dict:
                op_dict[key] = []

            param_list = op_dict[key]

            if isinstance(val, list):
                param_list.extend(val)
            else:
                param_list.append(val)

    @property
    def operations(self):

        # construct the $set changes
        fields = {}
        for key in self.__dirty_fields:

            latest_val = self.__attributes[key]

            #convert a Document to a dict
            if isinstance(latest_val, Document):
                latest_val = latest_val.document_as_dict()

            #convert a set to a list (mongo doesn't do sets)
            if isinstance(latest_val, set):
                latest_val = list(latest_val)

            # if its a list, convert any embedded docs to dicts!
            if isinstance(latest_val,list):
                newval = []
                for passed_thing in latest_val:
                    if isinstance(passed_thing, Document):
                        newval.append(passed_thing.document_as_dict())
                    else:
                        newval.append(passed_thing)
                latest_val = newval

            fields[self.long_to_short(key)] = latest_val

        fields_to_set = {'$set':fields}

        #merge the operations and the set of changes
        return dict(self.__ops.items() + fields_to_set.items())


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
        self.add_operation('$unset', key, 1)
        del self.__attributes[key]

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


    def save(self, safe=True):
        self.pre_save()
        new = not self.has_id

        # NOTE: this is called BEFORE we get self.operations
        # to allow the pre_ functions to add to the set of operations
        # for this object! (i.e. set last modified fields etc)
        if new:
            self.pre_insert()
        else:
            self.pre_update()

        # We execute the REQUIRED stuff AFTER the pre_save/insert/update
        # as those functions may well fill in the missing required fields!
        if not set(self.required).issubset(set(self.__attributes.keys())):
            raise ValueError('one or more required fields are missing: %s', set(self.required)-set(self.__attributes.keys()))


        col = self._dbcollection
        ops = self.operations

        if new:
            #if this is an insert, generate an ObjectId!
            if ops:
                res = col.find_and_modify(query={'_id':ObjectId()}, update=ops, upsert=True, new=True)
                self.load_dict(res)

            self.post_insert()
        else:
            if ops:
                res = col.find_and_modify(query={'_id':self.__attributes['_id']}, update=ops, upsert=True, new=True)
                self.load_dict(res)

            self.post_update()

        self.clear_ops()
        self.post_save()

    # delete this document from the collection
    def delete(self):
        if not self.has_id:
            raise ValueError("document has no _id")
        self._dbcollection.remove({'_id':self['_id']})


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


    # searching and what not
    @classmethod
    def find(cls, *args, **kwargs):
        """Class method that finds stuff somehow.
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


    # you can pass an ObjectId in and it'll auto-search on the _id field!
    @classmethod
    def find_one(cls, spec_or_id=None, *args, **kwargs):
        """Find a document with the given ObjectID `spec_or_id`.
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

    #TODO: implement update
    @classmethod
    def update(cls, spec, document, **kwargs):
        return cls._dbcollection.update(spec, document, **kwargs)



    # get a document by a specific id
    # this is mapped to the _id field
    # you can pass a string or an ObjectId
    @classmethod
    def get_by_id(cls, oid):
        #convert id to ObjectId
        if isinstance(oid, basestring):
            try:
                oid = ObjectId(oid)
            except:
                return None
        elif not isinstance(oid, ObjectId):
            raise ValueError('oid should be an ObjectId or string')

        return cls.find_one({'_id':oid})


    def to_json_dict(self, **kwargs):
        '''
        this is so an API can export the document as a JSON dict
        override this in your class call the super class
        ret = super(Class, self).to_json_dict(full_export)
        ret['yourthing']=self.thing
        lets you hide specific variables that dont need to be exported
        '''
        return OrderedDict()

    def from_json_dict(self, json_dict):
        return OrderedDict()


    #change tracking stuff calls this
    #TODO: needs to be more advanced
    def _mark_as_changed(self, key, val):
        self.mark_dirty(key)

    def document_as_dict(self):
        retdict = {}
        for key, val in self.__attributes.iteritems():
            field = self.field_map[key]
            short = self.long_to_short(key)
            retdict[short] = field.collapse(val)
        return retdict
