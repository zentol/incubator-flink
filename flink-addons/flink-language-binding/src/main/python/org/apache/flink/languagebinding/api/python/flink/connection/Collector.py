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
from abc import ABCMeta, abstractmethod
import struct

from flink.connection.RawConstants import Constants


class Collector(object):
    __metaclass__ = ABCMeta

    def __init__(self, con):
        self._connection = con

    @abstractmethod
    def collect(self, value):
        """
        Emits a record.

        :param value: The record to collect.:
        """
        pass

    @abstractmethod
    def _send_end_signal(self):
        pass


class RawCollector(Collector):
    def __init__(self, con):
        super(RawCollector, self).__init__(con)
        self._init = True
        self._cache = None

    def collect(self, value):
        if self._init:
            self._cache = value
            self._init = False
        else:
            self._send_record(False)
            self._cache = value

    def _send_end_signal(self):
        signal = struct.pack(">i", 64)
        self._connection.send(signal[3])

    def _finish(self):
        if not self._init:
            self._send_record(True)
            self._init = True
        else:
            self._send_end_signal()

    def _send_record(self, last):
        meta = 0
        if last:
            meta |= 32
        if not isinstance(self._cache, (list, tuple)):
            meta = struct.pack(">i", meta)
            self._connection.send(meta[3])
            self._send_field(self._cache)
        else:
            meta += len(self._cache)
            meta = struct.pack(">i", meta)
            self._connection.send(meta[3])
            for field in self._cache:
                self._send_field(field)

    def _send_field(self, value):
        if value is None:
            type = struct.pack(">B", Constants.TYPE_NULL)
            self._connection.send(type)
        elif isinstance(value, basestring):
            type = struct.pack(">B", Constants.TYPE_STRING)
            value = value.encode("utf-8")
            size = struct.pack(">I", len(value))
            self._connection.send("".join([type, size, value]))
        elif isinstance(value, bool):
            type = struct.pack(">B", Constants.TYPE_BOOLEAN)
            data = struct.pack(">?", value)
            self._connection.send("".join([type, data]))
        elif isinstance(value, int):
            type = struct.pack(">B", Constants.TYPE_INTEGER)
            data = struct.pack(">i", value)
            self._connection.send("".join([type, data]))
        elif isinstance(value, long):
            type = struct.pack(">B", Constants.TYPE_LONG)
            data = struct.pack(">l", value)
            self._connection.send("".join([type, data]))
        elif isinstance(value, float):
            type = struct.pack(">B", Constants.TYPE_DOUBLE)
            data = struct.pack(">d", value)
            self._connection.send("".join([type, data]))
        else:
            type = struct.pack(">B", Constants.TYPE_STRING)
            size = struct.pack(">I", len(str(value)))
            self._connection.send("".join([type, size, str(value)]))