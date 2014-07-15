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
from abc import ABCMeta


class InputIdentifier(object):
    TEXT = "text"
    JDBC = "jdbc"
    CSV = "csv"
    VALUE = "value"


class _InputFormat(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._arguments = []
        self._identifier = None


class TextInputFormat(_InputFormat):
    def __init__(self, file_path):
        super(TextInputFormat, self).__init__()
        self._identifier = InputIdentifier.TEXT
        self._arguments += (file_path,)


class JDBCInputFormat(_InputFormat):
    def __init__(self, drivername, url, query, username=None, password=None):
        super(JDBCInputFormat, self).__init__()
        self._identifier = InputIdentifier.JDBC
        self._arguments.append(drivername)
        self._arguments.append(url)
        self._arguments.append(query)
        if username is not None:
            self._arguments.append(username)
        if password is not None:
            self._arguments.append(password)


class CSVInputFormat(_InputFormat):
    def __init__(self, file_path, types):
        super(CSVInputFormat, self).__init__()
        self._identifier = InputIdentifier.CSV
        self._arguments += [file_path]
        if not isinstance(types, (list, tuple)):
            self._arguments += (types,)
        else:
            self._arguments += types


class ValueInputFormat(_InputFormat):
    def __init__(self, values):
        super(ValueInputFormat, self).__init__()
        self._identifier = InputIdentifier.VALUE
        self._arguments = values