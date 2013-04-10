.. Mongotron documentation master file, created by
   sphinx-quickstart on Wed Apr 10 17:59:31 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Mongotron's documentation!
=====================================

Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`



.. automodule:: mongotron
    :members:




Type description mini-language
##############################

Basic
+++++

    ``None``:
        The associated property can be set to any value.

    ``int``, ``long``:
        The associated property must be an integral value. Note that ``int``
        and ``long`` mean the same thing and are interchangeable; either type
        will be accepted.

    ``float``:
        The associated property must be a floating point value.


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


Document class
##############

.. autoclass:: mongotron.Document
    :members:

