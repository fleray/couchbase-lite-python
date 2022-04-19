# Query.py
#
# Copyright (c) 2019 Couchbase, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from ._PyCBL import ffi, lib
from .common import *
from .Collections import *
from .Document import MutableDocument
import json

JSONLanguage = 0
N1QLLanguage = 1

ValueIndex = 0
FullTextIndex = 1

class Query (CBLObject):

    def __init__(self, database, queryString, language = N1QLLanguage):
        errorPos = ffi.new("int*")
        CBLObject.__init__(self,
                           lib.CBLDatabase_CreateQuery(database._ref,
                                                       language, 
                                                       stringParam(queryString),
                                                       errorPos, 
                                                       gError),
                           "Couldn't create query", gError)
        self.database = database
        self.columnCount = lib.CBLQuery_ColumnCount(self._ref)
        self.sourceCode = queryString
        self.listeners = set()

    def __repr__(self):
        return self.__class__.__name__ + "['" + self.sourceCode + "']"

    @property
    def explanation(self):
        return sliceToString(lib.CBLQuery_Explain(self._ref))

    @property
    def columnNames(self):
        if not "_columns" in self.__dict__:
            cols = []
            for i in range(self.columnCount):
                name = sliceToString( lib.CBLQuery_ColumnName(self._ref, i) )
                cols.append(name)
            self._columns = cols
        return self._columns

    def setParameters(self, params):
        doc = MutableDocument("foo")
        doc.setProperties(params)
        doc._prepareToSave()
        lib.CBLQuery_SetParameters(self._ref, lib.CBLDocument_Properties(doc._ref))

    def execute(self):
        """Executes the query and returns a Generator of QueryResult objects."""
        results = lib.CBLQuery_Execute(self._ref, gError)
        if not results:
            raise CBLException("Query failed", gError)
        try:
            lastResult = None
            while lib.CBLResultSet_Next(results):
                if lastResult:
                    lastResult.invalidate()
                lastResult = QueryResult(self, results)
                yield lastResult
        finally:
            lib.CBL_Release(results)

    # Listeners:

    def addListener(self, listener):
        handle = ffi.new_handle(listener)
        self.listeners.add(handle)
        c_token = lib.CBLQuery_AddChangeListener(self._ref, lib.queryListenerCallback, handle)
        return ListenerToken(self, handle, c_token)

    def removeListener(self, token):
        token.remove()


class JSONQuery (Query):
    def __init__(self, database, jsonQuery):
        if not isinstance(jsonQuery, str):
            jsonQuery = encodeJSON(jsonQuery)
        Query.__init__(self, database, jsonQuery, JSONLanguage)


class N1QLQuery (Query):
    def __init__(self, database, n1ql):
        Query.__init__(self, database, n1ql, N1QLLanguage)


class QueryResult (object):
    """A container representing a query result. It can be indexed using either
       integers (to access columns in the order they were declared in the query)
       or strings (to access columns by name.)"""
    def __init__(self, query, results):
        self.query = query
        self._ref = results

    def __repr__(self):
        if self._ref == None:
            return "QueryResult[invalidated]"
        return "QueryResult" + encodeJSON(self.asDictionary())

    def invalidate(self):
        self._ref = None
        self.query = None

    def __len__(self):
        return self.query.columnCount

    def __getitem__(self, key):
        if self._ref == None:
            raise CBLException("Accessing a non-current query result row")
        if isinstance(key, int):
            if key < 0 or key >= self.query.columnCount:
                raise IndexError("Column index out of range")
            item = lib.CBLResultSet_ValueAtIndex(self._ref, key)
        elif isinstance(key, str):
            item = lib.CBLResultSet_ValueForKey(self._ref, key)
            if item == None:
                raise KeyError("No such column in Query")
        else:
            # TODO: Handle slices
            raise KeyError("invalid query result key")
        return decodeFleece(item)

    def __contains__(self, key):
        if self._ref == None:
            raise CBLException("Accessing a non-current query result row")
        if isinstance(key, int):
            if key < 0 or key >= self.query.columnCount:
                return False
            return (lib.CBLResultSet_ValueAtIndex(self._ref, key) != None)
        elif isinstance(key, str):
            return (lib.CBLResultSet_ValueForKey(self._ref, key) != None)
        else:
            return False

    def asArray(self):
        return decodeFleece(lib.CBLResultSet_ResultArray(self._ref))

    def asDictionary(self):
        return decodeFleece(lib.CBLResultSet_ResultDict(self._ref))


def createIndex(database, name, index_spec):
    type = None
    if index_spec != None:
        type = index_spec.type
        index_spec = index_spec._cblConfig()
    
    if type == ValueIndex:
        success = lib.CBLDatabase_CreateValueIndex(database._ref, stringParam(name), index_spec[0], gError)

    elif type == FullTextIndex:
        success = lib.CBLDatabase_CreateFullTextIndex(database._ref, stringParam(name), index_spec[0], gError)

    else:
        success = None

    if not success:
        raise CBLException("Index creation failed", gError)

def deleteIndex(database, name):    
    success = lib.CBLDatabase_DeleteIndex(database._ref, stringParam(name), gError)
    if not success:
        raise CBLException("Index deletion failed", gError)

# TODO - this is returning an empty array
def listIndexNames(database):    
    mutable_array = lib.CBLDatabase_IndexNames(database._ref)
    array = lib.FLMutableArray_GetSource(mutable_array)
    return decodeFleece(array)
    

class IndexSpec:
    def __init__(self, key_expressions, is_value_index = True, ignore_accents = False, language = None, query_language = lib.kCBLN1QLLanguage):
        if is_value_index:
            self.type = ValueIndex
        else:
            self.type = FullTextIndex
        if language is None:
            self.language = ffi.NULL
        else:
            self.language = stringParam(language)
        
        self.key_expressions_json = stringParam(encodeJSON(key_expressions))
        self.ignore_accents = ignore_accents
        self.query_language = query_language

    def _cblConfig(self):
        if self.type == ValueIndex:
            return ffi.new("CBLValueIndexConfiguration*",
                           [self.query_language,
                            self.key_expressions_json])

        elif self.type == FullTextIndex:
            return ffi.new("CBLFullTextIndexConfiguration*",
                           [self.query_language,
                            stringParam(expressions),
                            self.ignore_accents,
                            self.language])
        return None


@ffi.def_extern()
def queryListenerCallback(context, query):
    listener = ffi.from_handle(context)
    listener()
