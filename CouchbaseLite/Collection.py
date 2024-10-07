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
        """
        Returns the default collection of the default scope
        """
        gError = ffi.new("CBLError*")
        coll = lib.CBLDatabase_DefaultCollection(database._ref, gError)

        if not coll:
            raise CBLException("Couldn't return default collection", gError)
      
        return coll


    @staticmethod
    def get_collection(database, collection_name, scope_name):
        """
        Returns the collection named 'collection_name' inside scope 'scope_name' in the given database
        """
        gError = ffi.new("CBLError*")
        coll = lib.CBLDatabase_Collection(database._ref, stringParam(collection_name), stringParam(scope_name), gError)

        if not coll:
            raise CBLException("Couldn't return collection {}".format(stringParam(collection_name)), gError)
      
        return coll


    @staticmethod
    def create_collection(database, collection_name, scope_name):
        """
        Create a new collection named 'collection_name' inside scope 'scope_name' in the given database
        """
        gError = ffi.new("CBLError*")
        coll = lib.CBLDatabase_CreateCollection(database._ref, stringParam(collection_name), stringParam(scope_name), gError)

        if not coll:
            raise CBLException("Couldn't create collection with the provided collection and scope names", gError)
      
        return coll
  

    @staticmethod
    def delete_collection(database, collection_name, scope_name):
        """
        Delete an existing collection named 'collection_name' inside scope 'scope_name' in the given database
        """
        gError = ffi.new("CBLError*")
        is_deleted = lib.CBLDatabase_DeleteCollection(database._ref, stringParam(collection_name), stringParam(scope_name), gError)

        if not is_deleted:
            raise CBLException("Couldn't delete collection with the provided collection and scope names", gError)
      
        return is_deleted
  

    @staticmethod
    def FL_array_to_string_array(FL_array):
        """
        Internal utility method
        """
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
    

    @staticmethod
    def get_default_scope(database):
        """
        Returns the default scope
        """
        gError = ffi.new("CBLError*")
        scope = lib.CBLDatabase_DefaultScope(database._ref, gError)

        if not scope:
            raise CBLException("Couldn't return default scope", gError)
      
        return scope


    def get_scope(database, scope_name):
        """
        Returns an existing scope with the given name.
        """
        gError = ffi.new("CBLError*")
        scope = lib.CBLDatabase_Scope(database._ref, stringParam(scope_name), gError)
        if not scope:
            raise CBLException("Couldn't get the scope", gError)
        
        return scope


    @staticmethod
    def get_document(collection, doc_id):
        """
        Returns document with doc key 'docid' from given colllection
        """
        gError = ffi.new("CBLError*")
        mutable_doc = lib.CBLCollection_GetDocument(collection,stringParam(doc_id), gError)

        if not mutable_doc:
            raise CBLException("Couldn't get document in collection", gError)
      
        return mutable_doc


    @staticmethod
    def get_mutable_document(collection, doc_id):
        """
        Returns a mutable document with doc key 'docid' from given colllection
        """
        gError = ffi.new("CBLError*")
        mutable_doc = lib.CBLCollection_GetMutableDocument(collection,stringParam(doc_id), gError)

        if not mutable_doc:
            raise CBLException("Couldn't get mutable document in collection", gError)
      
        return mutable_doc


    @staticmethod
    def save_document(collection, doc):
        """
        Save a (mutable) document 'doc' inside the given 'colllection'
        """
        gError = ffi.new("CBLError*")
        save_doc = lib.CBLCollection_SaveDocument(collection, doc, gError)

        if not save_doc:
            raise CBLException("Couldn't save document in collection", gError)
      
        return save_doc
        
    @staticmethod
    def delete_document(collection, doc):
        """
        Delete the given document 'doc' inside the given 'colllection'
        """
        gError = ffi.new("CBLError*")
        is_deleted = lib.CBLCollection_DeleteDocument(collection, doc, gError)

        if not is_deleted:
            raise CBLException("Couldn't delete document in collection", gError)
      
        return is_deleted
    

    @staticmethod
    def purge_document(collection, doc):
        """
        Purge the given document 'doc' inside the given 'colllection'
        """
        gError = ffi.new("CBLError*")
        is_purged = lib.CBLCollection_PurgeDocument(collection, doc, gError)

        if not is_purged:
            raise CBLException("Couldn't purge document in collection", gError)
      
        return is_purged
    

    @staticmethod
    def purge_document_by_id(collection, doc_id):
        """
        Purge the given document with doc key "doc_id' inside the given 'colllection'
        """
        gError = ffi.new("CBLError*")
        is_purged = lib.CBLCollection_PurgeDocumentByID(collection, stringParam(doc_id), gError)

        if not is_purged:
            raise CBLException("Couldn't purge document by doc_id {} in collection".format(stringParam(doc_id)), gError)
      
        return is_purged
    

    @staticmethod
    def get_document_epxiration(collection, doc_id):
        """
        Returns the time, if any, at which a given document will expire and be purged.
        """
        gError = ffi.new("CBLError*")
        time_stamp = lib.CBLCollection_GetDocumentExpiration(collection, doc_id, gError)

        if not time_stamp:
            raise CBLException("Couldn't get the TTL for the document with doc_id {} in the given collection"
                               .format(stringParam(doc_id)), gError)
      
        return time_stamp
    

    @staticmethod
    def set_document_epxiration(collection, doc_id, expiration_ts):
        """
        Sets or clears the expiration time of a document. 
        """
        gError = ffi.new("CBLError*")
        is_TTL_set = lib.CBLCollection_SetDocumentExpiration(collection, doc_id, expiration_ts, gError)

        if not is_TTL_set:
            raise CBLException("Couldn't set the TTL {} for the document with doc_id {} in the given collection"
                               .format(stringParam(expiration_ts), stringParam(doc_id)), gError)
      
        return is_TTL_set