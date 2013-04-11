.. Mongotron documentation master file, created by
   sphinx-quickstart on Wed Apr 10 17:59:31 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Mongotron
=========

.. currentmodule:: mongotron

.. automodule:: mongotron
    :members:


10 second tutorial
##################

::

    import pymongo
    import mongotron

    mgr = mongotron.GetConnectionManager()
    mgr.add_connection(pymongo.Connection())

    class Doc(mongotron.Document):
        __db__ = 'test'
        structure = {
            'name': unicode,
            'age': int,
            'picture': bytes,
            'events': [int]
        }

    doc = Doc()
    doc.name = 'Foop'
    doc.age = 99
    doc.save()


Type description mini-language
##############################

Basic
+++++

    ``None``:
        The value can be any value.

    ``bool``
        The property must be ``True`` or ``False``.

    ``str``, ``bytes``
        The property must be a Python 2.x ``str()`` or a Python 3.x ``bytes()``
        instance. The resulting value is is serialized as a BSON.Binary().

    ``unicode``:
        The property must be a Unicode string.

    ``int``, ``long``:
        The property must be an integral value. Note that ``int`` and ``long``
        mean the same thing and are interchangeable; either type will be
        accepted.

    ``float``:
        The property must be a floating point value.


Documents
+++++++++

Any :py:class:`Document` subclass can be used as a type. In this case, the
:py:meth:`Document.document_as_dict` is invoked on it and the resulting dict is
stored in the parent :py:class:`Document`.

[TBD: limitations, etc.]


Lists
+++++

All list variants are ultimately persisted using the BSON array type.

    ``[]``
        The associated property must be a list of zero or more values.

    ``[basic type]``
        The associated property must be a list of zero or more `[basic type]`
        values.

    ``[basic type1, basic type2, ...]``
        The associated property must be a list with an identical length, and
        elements at each index must match the types in this list at the
        corresponding index.

        ::

            >>> # Value must be an array like [1, 1.1, u"test"].
            >>> [int, float, unicode]

    ``set``
        The associated property must be a set of zero or more values.

    ``set([basic type])``
        The associated proeprty must be a set of zero or more `[basic type]`
        values.


Specifying defaults
###################

Your :py:class:`Document` subclass may contain a
:py:attr:`Document.default_values` dict, which maps the names of fields to a
default value for them. Note the dict's contents are merged with (and
supercede) the contents of any base classes.


Specifying required fields
##########################

Your :py:class:`Document` subclass may contain a
:py:attr:`Document.required_fields` list, which lists the names of all fields
that must be set prior to save. Note the list's contents are merged with the
contents of any base classes.


Handling of ``None``
####################

Mongotron hates ``None`` and you probably should too. Setting a field to
``None`` is equivalent to deleting it. To really actually store ``null`` in the
database, use :py:meth:`Document.set` to unconditionally set a value.


Document class
##############

Saved-vs-unsaved
++++++++++++++++

.. autoattribute:: mongotron.Document._id


Reference
+++++++++

.. autoclass:: mongotron.Document
    :members:


DocumentMeta class
##################

.. autoclass:: mongotron.Document.DocumentMeta


Field class
###########

.. currentmodule:: mongotron.field_types

.. autoclass:: Field ()
    :members:


Field types
###########

.. autoclass:: BlobField ()
.. autoclass:: BoolField ()
.. autoclass:: DatetimeField ()
.. autoclass:: DictField ()
.. autoclass:: DocumentField ()
.. autoclass:: FixedListField ()
.. autoclass:: FloatField ()
.. autoclass:: IntField ()
.. autoclass:: ListField ()
.. autoclass:: ObjectIdField ()
.. autoclass:: SetField ()
.. autoclass:: TextField ()
