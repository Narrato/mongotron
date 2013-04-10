from ConnectionManager import GetConnectionManager
from bson.objectid import ObjectId, InvalidId
from Cursor import Cursor

from ChangeTrackingList import ChangeTrackingList
from ChangeTrackingDict import ChangeTrackingDict

from collections import OrderedDict


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
    def __new__(cls, name, bases, attrs):
        for base in bases:
            parent = base.__mro__[0]
            #determine if the parent has a structure dict
            structure = getattr(parent, 'structure', {})
            if hasattr(parent, 'structure'):
                if isinstance(parent.structure, dict):
                    #if parent.structure:
                    if 'structure' not in attrs and parent.structure:
                        attrs['structure'] = parent.structure
                    else:
                        obj_structure = attrs.get('structure', {}).copy()
                        attrs['structure'] = parent.structure.copy()
                        attrs['structure'].update(obj_structure)

            if getattr(parent, 'default_values', None):
                obj_default_values = attrs.get('default_values', {}).copy()
                attrs['default_values'] = parent.default_values.copy()
                attrs['default_values'].update(obj_default_values)

            if hasattr(parent, 'field_map'):
                if 'field_map' not in attrs and parent.field_map:
                    attrs['field_map'] = parent.field_map
                else:
                    obj_field_map = attrs.get('field_map', {}).copy()
                    attrs['field_map'] = parent.field_map.copy()
                    attrs['field_map'].update(obj_field_map)

            if hasattr(parent, 'required'):
                if attrs.get('required'):
                    attrs['required'] = list(set(parent.required).union(set(attrs['required'])))

        #print '----------------------------------------'
        #print attrs['structure']
        #print attrs['default_values']
        #print attrs['required']
        #print attrs['field_map']
        #print '----------------------------------------'
        #return type.__new__(cls, name, bases, attrs)

        ncls = type.__new__(cls, name, bases, attrs)
        if name == 'Document':
            return ncls

        ncls.initialize()
        return ncls


class Document(object):
    """A class with property-style access. It maps attribute access to an
    internal dictionary, and tracks changes.
    """

    __metaclass__ = DocumentMeta
    __should_explain = False
    __internalfields = ['_Document__attributes',
                        '_Document__ops',
                        '_Document__operations',
                        '_Document__dirty_fields']

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

    @classmethod
    def initialize(cls):
        #TODO: the mappings are duplicated due to historical reasons
        # (the find/find_one need to map too, but dont have access to an instance)
        # we should probably clean it up so there is only one copy eh?
        cls.db_name_to_field = {}
        cls.fieldname_to_dbname = {}

        for fieldname, v in cls.field_map.items():
            # we map the "object properties" to db_keys *which can be different*
            cls.db_name_to_field[v] = fieldname
            cls.fieldname_to_dbname[fieldname] = v

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
        the dictionary `doc`.
        """
        if default_keys is None:
            default_keys = set()
        self.__attributes = {}

        for k, v in doc.iteritems():
            k = self.short_to_long(k)

            value_type = self.structure.get(k, None)

            if value_type:
                if isinstance(value_type, list):
                    #todo: we expect a list of object types
                    if not isinstance(v, list):
                        raise ValueError('wrong type for %s wanted %s got %s' % (k, value_type, type(v)))

                    thingwewant = value_type[0]
                    newv = []
                    for passed_thing in v:

                        #TODO: replace this with something NICER PLEASE!!!
                        # INSTANTIATING THE TYPE IS HORRIBLE AND I AM SURE IT WILL BREAK
                        if issubclass(thingwewant, Document):
                            passed_thing = thingwewant(passed_thing)
                            #passed_thing._embedded = True;

                        newv.append(passed_thing)

                    v = newv
                    self.__attributes[k] = v
                #transforming things back into python types :o
                elif not isinstance(v, value_type):
                    #in theory this might convert a dict back into a document?
                    self.__attributes[k] = value_type(v)
                else:
                    self.__attributes[k] = v
            else:
                self.__attributes[k] = v

            #if a doc was passed in and it contains the default field, then lets not set it
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

        # get this from self.__attributes
        if self.key_in_structure(key):
            #TODO: wrap lists & dictionaries in a mutation tracking version
            # which can push changes back up into our change set pls!
            attr = self.__attributes.get(key, None)

            if attr is None:
                value_type = self.structure.get(key, None)

                #TODO: make this awesome please
                if isinstance( value_type, set ):
                    attr = set()
                elif isinstance( value_type, list ):
                    attr = ChangeTrackingList([], self, key)
                elif isinstance( value_type, dict ):
                    attr = ChangeTrackingDict({}, self, key)
            else:
                value_type = self.structure.get(key, None)

                #if the structure was a set() then make damned sure we return a set!
                if isinstance( attr, list ) and isinstance( value_type, set ):
                    attr = set(attr)
                if (isinstance(attr, (list, tuple)) and not isinstance(attr, ChangeTrackingList)):
                    attr = ChangeTrackingList(attr, self, key)
                elif isinstance(attr, dict) and not isinstance(attr, ChangeTrackingDict):
                    attr = ChangeTrackingDict(attr, self, key)

            return attr
        else:
            # Default behaviour
            return object.__getattribute__(self, key)

    def __setattr__(self, key, value):
        if self.key_in_structure(key):
            #todo check value is the right type as defined in self.structure
            value_type = self.structure[key]
            if value_type:
                if isinstance(value_type, list):
                    #todo: we expect a list of object types
                    if not isinstance(value,list):
                        raise ValueError('wrong type for %s wanted %s got %s' % (key, value_type, type(value)))

                    for passed_thing in value:
                        if not isinstance(passed_thing, value_type[0]):
                            raise ValueError('wrong type for %s wanted %s got %s' % (key, value_type, type(value)))

                elif not isinstance(value, value_type):
                    raise ValueError('wrong type for %s wanted %s got %s' % (key, value_type, type(value)))

            self.set(key, value)
        else:
            if key in self.__internalfields:
                return super(Document, self).__setattr__(key, value)
            raise ValueError('%s this is not a settable key' % key)

    #TODO: turn __delattr__ into a $unset?

    # mongo operation wrappers!
    def add_operation(self, op, key, val):
        """Arrange for the Mongo operation `op` to be applied to the document
        property `key` with the operand `value` during save.
        """
        if key not in self.structure:
            raise KeyError('%r is not a settable key' % (key,))

        # convert an assigned doc into a dict pls
        # we should also iterate over lists doing the same thing :/
        # also what about converting the other way?
        if isinstance(val, Document):
            val = val.document_as_dict()

        #TODO: get the value_type, if its an instance of a list
        # then if the contents is a subclass of Document
        # then transport that to a list of dicts
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

        # we should probably make this smarter so you can't
        # set a top level array and a component at the same time
        # though if you're doing that, your code is broken anyway

        key = self.long_to_short(key)

        if not op in self.__ops:
            self.__ops[op] = {}

        op_dict = self.__ops[op]
        if(op == '$set'):
            raise ValueError('$set is an invalid operation!')
        elif(op == '$addToSet'):
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
            if not key in op_dict:
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

        if value is None:
            self.unset(key)
            return

        # this allows you to SPECIFICALLY bypass the property checking and
        # set a field directly, even if its not defined
        #self.add_operation('$set', key, value)
        self.__dirty_fields.add(key)

        # set it in the attributes dict too
        self.__attributes[key] = value

    def unset(self, key):
        # this allows you to SPECIFICALLY bypass the property checking and
        # set a field directly, even if its not defined
        self.add_operation('$unset', key, 1)
        # set it in the attributes dict too
        del self.__attributes[key]


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
    def cls_long_to_short(cls, long_key):
        short_key = cls.fieldname_to_dbname.get(long_key, None)
        if not short_key:
            short_key = long_key
        return short_key

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

            k = cls.cls_long_to_short(k)
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


        #nothing to do
    def from_json_dict(self, json_dict):
        return OrderedDict()


    #change tracking stuff calls this
    #TODO: needs to be more advanced
    def _mark_as_changed(self, key, val):
        self.mark_dirty(key)

    # this is for embedding a document in another document, it convert
    # the document into a dict so it can be embedded
    def export_list_to_dict(self, thelist):
        newlist = []
        for item in thelist:
            if isinstance(item, Document):
                item = item.document_as_dict()
            newlist.append(item)
        return newlist


    def document_as_dict(self):
        retdict = {}
        for key in self.__attributes:
            val = self.__attributes[key]

            if isinstance(val, Document):
                val = val.document_as_dict()
            if isinstance(val, list):
                val = self.export_list_to_dict(val)
            #TODO: anything else we should care about?

            retdict[self.long_to_short(key)] = val

        return retdict
