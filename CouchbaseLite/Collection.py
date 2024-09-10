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
#

import datetime
import math
from typing import Union, List

from ._PyCBL import ffi, lib
from .common import *
from .Document import *

class Collection:
      def __init__(self, database):
        outError = ffi.new("CBLError*")
        CBLObject.__init__(self,
                           lib.CBLDatabase_DefaultCollection(database._ref,
                                                       outError),
                           "Couldn't create default collection", outError)
        self.database = database
