#! /usr/bin/env python3
#
#  test.py
#
# Copyright (c) 2019-2021 Couchbase, Inc. All rights reserved.
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

from CouchbaseLite.Database import Database, DatabaseConfiguration, IndexConfiguration, FullTextIndexConfiguration
from CouchbaseLite.Document import Document, MutableDocument
from CouchbaseLite.Query import N1QLQuery, QueryResult, JSONLanguage
#from CouchbaseLite.Replicator import ReplicatorConfiguration, ReplicatorType, Replicator
from CouchbaseLite.Replicator import ReplicatorConfiguration, Replicator, ReplicatorType, ReplicationCollection
from CouchbaseLite.Collection import Collection

import json, time, uuid, sys
import SensorSimulator

NUM_PROBES = 4


def create_new_database(db_name = 'my-database-made-using-python-wrapper'):
    db = Database(db_name, DatabaseConfiguration('.'))
    return db


def close_database(db):
    db.close()


def add_log(db, log_message):
    tus = time.time()
    doc_id = 'log::{}'.format(tus)

    with db:
        doc = db.getMutableDocument(doc_id)
        props = doc.properties

        props['type'] = 'log'
        props['message'] = log_message
        props['timestamp'] = '{}'.format(tus)

        db.saveDocument(doc)


def save_doc_inside_collection(db, sensor_id, collection, json_doc):
    tus = int(time.time_ns() / 1000000)
    doc_id = 'sensor::{}::{}'.format(sensor_id, uuid.uuid4())

    doc = Document.createDocWithId(doc_id)
    Document.setJSON(doc, json_doc)

    with db:
        Collection.save_document(collection, doc)

        # Code below are just to test document deletion and purge are working:
        #Collection.delete_document(collection, doc)
        #Collection.purge_document(collection, doc)


def add_new_json_sample(db, sensor_id, last_value):
    
    coll_temp = Collection.get_collection(db, "temperatures", "measures")
    coll_press = Collection.get_collection(db, "pressures", "measures")

    prob_properties= SensorSimulator.generate_json_doc(last_value, sensor_id)

    save_doc_inside_collection(db, sensor_id, coll_temp, prob_properties)
    save_doc_inside_collection(db, sensor_id, coll_press, prob_properties)


def select_count(db, scope_and_collection):
    q = N1QLQuery(db, 'SELECT count(*) AS count FROM {}'.format(scope_and_collection))

    count_result = None

    for row in q.execute():
        count_result = row.asDictionary()
        print('-> {}: {}'.format(scope_and_collection, count_result))


def start_replication(db: Database, endpoint_url, username, password):
    
    # get default scope
    default_scope = Collection.get_default_scope(db)
    print('Default scope {}'.format(default_scope))

    # get default collection
    default_coll = Collection.get_default_collection(db)
    print('Default collection {}'.format(default_coll))

    coll_temp = Collection.create_collection(db, "temperatures", "measures")
    coll_press = Collection.create_collection(db, "pressures", "measures")
    dummy_collection = Collection.create_collection(db, "dummy", "measures")

    # get scopes/collections names before 'dummy' collection deletion
    scope_names = Collection.get_scope_names(db)
    print('LIST SCOPES and associated COLLECTIONS')
    for scope in scope_names:
        collection_names = Collection.get_collection_names(db, scope)
        for coll in collection_names:
            print('Inside scope {} -> collection {}'.format(scope, coll))

    if Collection.delete_collection(db, "dummy", "measures"):
        print('Dummy collection has been successfully deleted')

    # get scopes/collections names :
    scope_names = Collection.get_scope_names(db)
    print('LIST SCOPES and associated COLLECTIONS')
    for scope in scope_names:
        collection_names = Collection.get_collection_names(db, scope)
        for coll in collection_names:
            print('Inside scope {} -> collection {}'.format(scope, coll))

    # get collection named 'temperatures' in scope measures
    coll_temp2 = Collection.get_collection(db, "temperatures", "measures")
    assert coll_temp == coll_temp2

    replica_param_coll_temp =  {'collection': coll_temp,  'push_filter': None, 'pull_filter': None, 'conflict_resolver': None, 'channels': None, 'documentIDs': None}
    replica_param_coll_press = {'collection': coll_press, 'push_filter': None, 'pull_filter': None, 'conflict_resolver': None, 'channels': None, 'documentIDs': None}

    rep_coll = ReplicationCollection([replica_param_coll_temp, replica_param_coll_press])
    
    # database, url, push_filter, pull_filter, conflict_resolver, username, password, cert_path, collections, collection_count
    replicator_cfg = ReplicatorConfiguration(None, endpoint_url, None, None, None, username, password, None, rep_coll, 2)
    replicator_cfg.collection_count = 2
    replicator_cfg.replicator_type = ReplicatorType.CBLReplicatorTypePushAndPull
    replicator_cfg.continuous = True

    replicator = Replicator(replicator_cfg)
    replicator.start(resetCheckpoint=False)

    return replicator


def main():
    print("Hello CB Lite Python sample code!")
    
    if len(sys.argv) != 4:
        print('One string argument specifying the AppServices endpoint is needed.')
        print('len(sys.argv) = {}'.format(len(sys.argv)))
        print('Values are:')
        for sys_arg in sys.argv:
            print(sys_arg)
        return -1    

    endpoint_url = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]

    db = create_new_database() # if needed

    replicator = start_replication(db, endpoint_url, username, password)

    last_values = []
    for x in range(NUM_PROBES):
        last_values.append(- sys.float_info.max)

    while True:
        for x in range(NUM_PROBES):
            sensor_id = x
            add_new_json_sample(db, sensor_id, last_values[x])

            time.sleep(2)
            select_count(db, 'measures.temperatures') # list n temperatures documents inside local CBlite DB
            select_count(db, 'measures.pressures') # list n pressures documents inside local CBlite DB
        #Replic

    close_database(db)


if __name__ == "__main__":
    main()