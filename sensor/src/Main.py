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


def create_new_database(db_name = 'my-database'):
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

    
def add_new_json_sample(db, sensor_id, last_value):
    prob_properties= SensorSimulator.generate_json_doc(last_value, sensor_id)

    tus = int(time.time_ns() / 1000000)
    doc_id = 'sensor::{}::{}'.format(sensor_id, uuid.uuid4())

    with db:
        doc = MutableDocument(doc_id)
        doc.properties = prob_properties

        db.saveDocument(doc)

def select_count(db):
    q = N1QLQuery(db, 'SELECT count(*) AS count FROM _')

    count_result = None

    for row in q.execute():
        count_result = row.asDictionary()
        print(count_result)


def start_replication(db, endpoint_url, username, password):
    
    collection = Collection(db)
    replication_collection = ReplicationCollection(collection, None, None, None, None, None)
    
    replicator_cfg = ReplicatorConfiguration(db, endpoint_url, None, None, None, username, password, None, replication_collection, 1)
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
    select_count(db) # list n documents inside local CBlite DB
    add_new_json_sample(db, 1, 32) # basic test
    select_count(db) # list n+1 documents inside local CBlite DB

    replicator = start_replication(db, endpoint_url, username, password)

    last_values = []
    for x in range(NUM_PROBES):
        last_values.append(- sys.float_info.max)

    while True:
        for x in range(NUM_PROBES):
            sensor_id = x
            add_new_json_sample(db, sensor_id, last_values[x])

            time.sleep(2)
            select_count(db)

        #Replic

    close_database(db)

if __name__ == "__main__":
    main()