
from __future__ import absolute_import


class Error(Exception):
    """Most general error type raised by Mongotron. Currently unused.
    """


class ValidationError(Error):
    """An attempt was made to set or load a document field that did not match
    its schema.
    """
    def __init__(self, msg, path=None):
        Error.__init__(self, msg)
        #: Path to the erroneous field, from the root of the document being
        #: validated. Uses MongoDB-style "doc.bar.0.foo"
        self.path = path
