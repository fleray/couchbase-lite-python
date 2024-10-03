#! /bin/bash -e
#
# Convenience script to run `test.py` -- 
# just sets PYTHONPATH to point to the parent dire, so the CouchbaseLite package will be loaded.

SCRIPT_DIR=`dirname $0`
cd "$SCRIPT_DIR"

export PYTHONPATH=..
python3 src/Main.py wss://8se06xixb7lkv54r.apps.cloud.couchbase.com:4984/test-cblite-python demo P@ssw0rd!
