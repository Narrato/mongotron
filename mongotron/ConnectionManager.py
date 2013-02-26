#!/usr/bin/env python
# encoding: utf-8
"""
ConnectionManager.py

Created by Tony Million on 2013-01-27.
Copyright (c) 2013 Narrato. All rights reserved.
"""

#very simple class that "manages" the connections in use on this system
# its up to you as the user to add the connections - at the most basic you call
#  manager.add_connection(yourMongoClient)  which will register a default client

_conn_pool = {}

class ConnectionManager(object):

    def add_connection(self, connection, name='default'):
        _conn_pool[name] = connection
        pass

    def get_connection(self, name='default', default_if_none=False):
        if name in _conn_pool:
            return _conn_pool[name]

        if default_if_none:
            return _conn_pool['default']

        return None;
        
_manager = ConnectionManager()

def GetConnectionManager():
    return _manager
