from ._PyCBL import ffi, lib
from .common import *
import json

#class ReplicatorStatus:
#    def __init__(self, activity, progress, error)
#    ReplicatorActivityLevel = lib.CBLReplicatorActivityLevel


class ReplicatorType:
    CBLReplicatorTypePushAndPull = lib.kCBLReplicatorTypePushAndPull
    CBLReplicatorTypePush = lib.kCBLReplicatorTypePush
    CBLReplicatorTypePull = lib.kCBLReplicatorTypePull
    
class ReplicationCollection:
    
    def __init__(self, coll_array):
        size = len(coll_array)

        self._ref = ffi.new("CBLReplicationCollection["+str(size)+"]")
        for i in range(size):
            self._ref[i].collection = coll_array[i]['collection']
            if 'push_filter' in coll_array[i] and coll_array[i]['push_filter']:
                self._ref[i].pushFilter = coll_array[i]['push_filter']
            if 'pull_filter' in coll_array[i] and coll_array[i]['pull_filter']:
                self._ref[i].pullFilter = coll_array[i]['pull_filter']
            if 'conflict_resolver' in coll_array[i] and coll_array[i]['conflict_resolver']:
                self._ref[i].conflict_resolver = coll_array[i].conflict_resolver               
            if 'channels' in coll_array[i] and coll_array[i]['channels']:
                self._ref[i].channels = coll_array[i].channels
            if 'documentIDs' in coll_array[i] and coll_array[i]['documentIDs']:
                self._ref[i].documentIDs = coll_array[i].documentIDs

class ReplicatorConfiguration:
    def __init__(self, database, url, push_filter, pull_filter, conflict_resolver, username, password, cert_path, collections, collection_count):
        pinned_server_cert = []
        if cert_path:
            cert_as_bytes = open(cert_path, "rb").read()
            pinned_server_cert = [asSlice(cert_as_bytes)]

        self.database = database
        self.endpoint = lib.CBLEndpoint_CreateWithURL(stringParam(url), gError)
        self.replicator_type = ReplicatorType.CBLReplicatorTypePushAndPull
        self.continuous = True
        self.disable_auto_purge = True
        self.max_attempts = 0
        self.max_attempt_wait_time = 0
        self.heartbeat = 0
        self.authenticator = lib.CBLAuth_CreatePassword(stringParam(username), stringParam(password))
        self.proxy = ffi.NULL
        self.headers = ffi.NULL
        self.pinned_server_cert = pinned_server_cert
        self.trusted_root_cert = []
        self.channels = ffi.NULL
        self.document_ids = ffi.NULL
        self.push_filter = push_filter
        self.pull_filter = pull_filter
        self.conflict_resolver = conflict_resolver
        self.context = ffi.NULL
        if collections is not None:
            self.collections = collections # Required if the database is not set !!!
        else:
            self.collections = ffi.NULL
        self.collection_count = collection_count
        self.property_encryptor = ffi.NULL
        self.property_decryptor = ffi.NULL
        self.document_property_encryptor = ffi.NULL
        self.document_property_decryptor = ffi.NULL
        self.accept_parent_domain_cookies = False

    def _cblConfig(self):
        db = None
        if self.database is None:
            db = ffi.NULL
        else:
            db = self.database._ref

        replicator_config = ffi.new("CBLReplicatorConfiguration*")
        if self.database is not None:  #The databse to replicate with the target's endpoint (Required if the collections is not set).
            replicator_config.database = self.database._ref
        replicator_config.endpoint = self.endpoint
        replicator_config.replicatorType = self.replicator_type
        replicator_config.continuous = self.continuous
        replicator_config.disableAutoPurge = self.disable_auto_purge
        replicator_config.maxAttempts = self.max_attempts
        replicator_config.maxAttemptWaitTime = self.max_attempt_wait_time
        replicator_config.heartbeat = self.heartbeat
        replicator_config.authenticator = self.authenticator
        replicator_config.proxy = self.proxy
        replicator_config.headers = self.headers
        replicator_config.pinnedServerCertificate = self.pinned_server_cert
        replicator_config.trustedRootCertificates = self.trusted_root_cert
        replicator_config.channels = self.channels
        replicator_config.documentIDs = self.document_ids

        if self.push_filter is not None:
            replicator_config.pushFilter = self.push_filter

        if self.pull_filter is not None:
            replicator_config.pullFilter = self.pull_filter

        if self.conflict_resolver is not None:
            replicator_config.conflictResolver = self.conflict_resolver

        replicator_config.context = self.context
        replicator_config.propertyEncryptor = self.property_encryptor
        replicator_config.propertyDecryptor = self.property_decryptor
        replicator_config.documentPropertyEncryptor = self.document_property_encryptor
        replicator_config.documentPropertyDecryptor = self.document_property_decryptor
        if self.collections is not None:
            replicator_config.collections = self.collections._ref #The collections to replicate with the target's endpoint (Required if the database is not set).
        replicator_config.collectionCount = self.collection_count
        replicator_config.acceptParentDomainCookies = self.accept_parent_domain_cookies

        return replicator_config


class Replicator (CBLObject):
    
    def __init__(self, config):
        if config != None:
            config = config._cblConfig()
        CBLObject.__init__(self,
                           lib.CBLReplicator_Create(config, gError),
                           "Couldn't create replicator", gError)
        self.config = config

    def start(self, resetCheckpoint = False):
        lib.CBLReplicator_Start(self._ref, resetCheckpoint)

    def stop(self):
        lib.CBLReplicator_Stop(self._ref)
