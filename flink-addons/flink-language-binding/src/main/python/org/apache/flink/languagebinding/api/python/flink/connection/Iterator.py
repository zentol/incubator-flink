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
from collections import deque
import struct

from flink.connection.RawConstants import Constants


class Iterator(object):
    __metaclass__ = ABCMeta

    def __init__(self, con):
        self._connection = con

    @abstractmethod
    def has_next(self):
        """
        Provides information whether this iterator contains another element.

        :return: True, if this iterator contains another element, false otherwise.
        """
        pass

    @abstractmethod
    def next(self):
        """
        Returns the next element in this iterator.

        :return: The next element.
        """
        pass

    @abstractmethod
    def all(self):
        """
        Returns all remaining elements in this iterator.
        :return: A list containing all remaining elements.
        """
        pass

    @abstractmethod
    def _reset(self):
        pass


class RawIterator(Iterator):
    def __init__(self, con):
        super(RawIterator, self).__init__(con)
        self._cache = deque()
        self._cache_mode = 0
        self._was_last = [False, False]

    def next(self, group=0):
        if len(self._cache) > 0 & group == self._cache_mode:
            return self._cache.popleft()
        raw_meta = "\x00\x00\x00" + self._connection.receive(1)
        meta = struct.unpack(">I", raw_meta)[0]
        if meta == 64:
            self._was_last[group] = True
            return None
        if meta == 128:
            return True
        record_group = meta >> 7
        if (meta & 32) == 32:
            self._was_last[group] = True
        size = meta & 31
        if record_group == group:
            if size == 31:
                return self._receive_field()
            result = ()
            for i in range(size):
                result += (self._receive_field(),)
            return result
        else:
            if size == 31:
                self._cache.append(self._receive_field())
            else:
                result = ()
                for i in range(size):
                    result += (self._receive_field(),)
                if len(self._cache) == 0:
                    self._cache_mode = group
                self._cache.append(result)
            return self.next(group)

    def _receive_field(self):
        raw_type = "\x00\x00\x00" + self._connection.receive(1)
        type = struct.unpack(">i", raw_type)[0]
        if (type == Constants.TYPE_BOOLEAN):
            raw_bool = self._connection.receive(1)
            return struct.unpack(">?", raw_bool)[0]
        elif (type == Constants.TYPE_BYTE):
            raw_byte = self._connection.receive(1)
            return struct.unpack(">c", raw_byte)[0]
        elif (type == Constants.TYPE_FLOAT):
            raw_float = self._connection.receive(4)
            return struct.unpack(">f", raw_float)[0]
        elif (type == Constants.TYPE_DOUBLE):
            raw_double = self._connection.receive(8)
            return struct.unpack(">d", raw_double)[0]
        elif (type == Constants.TYPE_SHORT):
            raw_short = self._connection.receive(2)
            return struct.unpack(">h", raw_short)[0]
        elif (type == Constants.TYPE_INTEGER):
            raw_int = self._connection.receive(4)
            return struct.unpack(">I", raw_int)[0]
        elif (type == Constants.TYPE_LONG):
            raw_long = self._connection.receive(8)
            return struct.unpack(">l", raw_long)[0]
        elif (type == Constants.TYPE_STRING):
            raw_size = self._connection.receive(4)
            size = struct.unpack(">I", raw_size)[0]
            if size == 0:
                return ""
            return self._connection.receive(size).decode("utf-8")
        elif (type == Constants.TYPE_NULL):
            return None

    def all(self, group=0):
        values = []
        if self._cache_mode == group:
            while len(self._cache) > 0:
                values.append(self._cache.popleft())
        while self.has_next():
            values.append(self.next())
        return values

    def has_next(self, group=0):
        return not self._was_last[group]

    def _reset(self):
        self._was_last = [False, False]


class Dummy(Iterator):
    def __init__(self, iterator, group):
        super(Dummy, self).__init__(iterator._connection)
        self._iterator = iterator
        self._group = group

    def next(self):
        return self._iterator.next(self._group)

    def has_next(self):
        return self._iterator.has_next(self._group)

    def all(self):
        return self._iterator.all(self._group)

    def _reset(self):
        self._iterator._reset()
