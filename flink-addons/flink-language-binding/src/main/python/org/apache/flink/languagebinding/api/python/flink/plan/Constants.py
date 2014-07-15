################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################


class _Operations(object):
    """
    Gotta be kept in sync with java constants!
    """
    SORT = "sort"
    GROUP = "groupby"
    COGROUP = "cogroup"
    CROSS = "cross"
    CROSSH = "cross_h"
    CROSST = "cross_t"
    FLATMAP = "flatmap"
    FILTER = "filter"
    GROUPREDUCE = "groupreduce"
    JOIN = "join"
    JOINH = "join_h"
    JOINT = "join_t"
    MAP = "map"
    PROJECTION = "projection"
    REDUCE = "reduce"
    UNION = "union"


class _Fields(object):
    FORMAT = "format"
    PARENT = "parent"
    CHILD = "new_set"
    OTHER = "other_set"
    SET = "set"
    IDENTIFIER = "identifier"
    FIELD = "field"
    ORDER = "order"
    KEYS = "keys"
    KEY1 = "key1"
    KEY2 = "key2"
    PKEY1 = "pkey1"
    PKEY2 = "pkey2"
    TYPES = "types"
    OPERATOR = "operator"
    META = "meta"
    NAME = "name"


class WriteMode(object):
    NO_OVERWRITE = 0
    OVERWRITE = 1


class Order(object):
    NONE = 0
    ASCENDING = 1
    DESCENDING = 2
    ANY = 3


class Types(object):
    BOOL = True
    INT = 1
    LONG = 1L
    FLOAT = 2.5
    STRING = "type"