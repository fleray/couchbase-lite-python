# Collection.py
#
# Copyright (c) 2019-2024 Couchbase, Inc. All rights reserved.
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


import datetime
import math
from typing import Union, List

from ._PyCBL import ffi, lib
from .common import *
from .Document import *
 

class Collection:
    """
    New methods for scopes and collections
    """
    @staticmethod
    def get_default_collection(database):
        gError = ffi.new("CBLError*")
        coll = lib.CBLDatabase_DefaultCollection(database._ref, gError)

        if not coll:
            raise CBLException("Couldn't return default collection", gError)
      
        return coll


    @staticmethod
    def create_collection(database, collection_name, scope_name):
        gError = ffi.new("CBLError*")
        coll = lib.CBLDatabase_CreateCollection(database._ref, stringParam(collection_name), stringParam(scope_name), gError)

        if not coll:
            raise CBLException("Couldn't create collection with the provided collection and scope names", gError)
      
        return coll
  

    @staticmethod
    def FL_array_to_string_array(FL_array):
        iter = ffi.new("FLArrayIterator*")
        array = ffi.cast("FLArray", FL_array)
        lib.FLArrayIterator_Begin(array, iter)

        string_results = []

        while lib.FLArrayIterator_GetValue(iter):
            value = lib.FLArrayIterator_GetValue(iter)
            tostr = lib.FLValue_ToString(value)
            char_array_size = tostr.size + 1

            char_array = ffi.new("char["+ str(char_array_size) +"]")
            lib.FLSlice_ToCString(lib.FLSliceResult_AsSlice(tostr), char_array, char_array_size)
            str_result = ffi.string(char_array).decode('utf-8')
            string_results.append(str_result)

            lib.FLArrayIterator_Next(iter)

        return string_results


    @staticmethod
    def get_scope_names(database):
        """
        Returns all scope names inside the given database
        """
        gError = ffi.new("CBLError*")
        mutable_array = lib.CBLDatabase_ScopeNames(database._ref, gError)
        if not mutable_array:
            raise CBLException("Couldn't get scope names", gError)
        
        string_results = Collection.FL_array_to_string_array(mutable_array)
        
        return string_results


    @staticmethod
    def get_collection_names(database, scope_name) -> List[str]:
        """
        Returns all collection names inside the given scope
        """
        gError = ffi.new("CBLError*")
        mutable_array = lib.CBLDatabase_CollectionNames(database._ref, stringParam(scope_name), gError)
        if not mutable_array:
            raise CBLException("Couldn't get collection names", gError)
        
        string_results = Collection.FL_array_to_string_array(mutable_array)
        
        return string_results
    
    # Returns an existing scope with the given name.
    def get_scope(database, scope_name):
        gError = ffi.new("CBLError*")
        scope = lib.CBLDatabase_Scope(database._ref, stringParam(scope_name), gError)
        if not scope:
            raise CBLException("Couldn't get the scope", gError)
        
        return scope


    @staticmethod
    def get_document(collection, doc_id):
        gError = ffi.new("CBLError*")
        mutable_doc = lib.CBLCollection_GetDocument(collection,stringParam(doc_id), gError)

        if not mutable_doc:
            raise CBLException("Couldn't get document in collection", gError)
      
        return mutable_doc


    @staticmethod
    def get_mutable_document(collection, doc_id):
        gError = ffi.new("CBLError*")
        mutable_doc = lib.CBLCollection_GetMutableDocument(collection,stringParam(doc_id), gError)

        if not mutable_doc:
            raise CBLException("Couldn't get mutable document in collection", gError)
      
        return mutable_doc


    @staticmethod
    def save_document(collection, doc):
        gError = ffi.new("CBLError*")
        save_doc = lib.CBLCollection_SaveDocument(collection, doc, gError)

        if not save_doc:
            raise CBLException("Couldn't save document in collection", gError)
      
        return save_doc
        