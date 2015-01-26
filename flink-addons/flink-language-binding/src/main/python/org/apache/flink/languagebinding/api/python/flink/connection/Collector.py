# ###############################################################################
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
from struct import pack
import sys

from flink.connection.Constants import Types

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    stringtype = basestring
else:
    stringtype = str

class Collector(object):
    def __init__(self, con):
        self._connection = con
        self._serializer = None

    def close(self):
        self._connection.send_end_signal()

    def collect(self, value):
        self._serializer = _get_serializer(self._connection.write, value)
        self.collect = self._collect
        self.collect(value)

    def _collect(self, value):
        self._connection.write(self._serializer.serialize(value))


def _get_serializer(write, value):
    if isinstance(value, (list, tuple)):
        write(Types.TYPE_TUPLE)
        write(pack(">I", len(value)))
        return TupleSerializer(write, value)
    elif value is None:
        write(Types.TYPE_NULL)
        return NullSerializer()
    elif isinstance(value, stringtype):
        write(Types.TYPE_STRING)
        return StringSerializer()
    elif isinstance(value, bool):
        write(Types.TYPE_BOOLEAN)
        return BooleanSerializer()
    elif isinstance(value, int) or PY2 and isinstance(value, long):
        write(Types.TYPE_LONG)
        return LongSerializer()
    elif isinstance(value, float):
        write(Types.TYPE_DOUBLE)
        return FloatSerializer()
    else:
        raise Exception("Unsupported Type encountered.")


class TupleSerializer(object):
    def __init__(self, write, value):
        self.serializer = [_get_serializer(write, field) for field in value]

    def serialize(self, value):
        bits = []
        for i in range(len(value)):
            bits.append(self.serializer[i].serialize(value[i]))
        return b"".join(bits)


class BooleanSerializer(object):
    def serialize(self, value):
        return pack(">?", value)


class FloatSerializer(object):
    def serialize(self, value):
        return pack(">d", value)


class LongSerializer(object):
    def serialize(self, value):
        return pack(">q", value)


class StringSerializer(object):
    def serialize(self, value):
        value = value.encode("utf-8")
        return pack(">I", len(value)) + value


class NullSerializer(object):
    def serialize(self, value):
        return b""


class TypedCollector(object):
    def __init__(self, con):
        self._connection = con
        if PY2:
            self.collect = self._collect2
        else:
            self.collect = self._collect3

    #special case method beacuse python sucks
    def collectBytes(self, value):
        size = pack(">I", len(value))
        self._connection.write(b"".join([Types.TYPE_BYTES, size, value]))

    def collect(self):
        pass

    def _collect2(self, value):
        if not isinstance(value, (list, tuple)):
            self._send_field2(value)
        else:
            self._connection.write(Types.TYPE_TUPLE)
            meta = pack(">I", len(value))
            self._connection.write(meta[3])
            for field in value:
                self._collect2(field)

    def _send_field2(self, value):
        if value is None:
            self._connection.write(Types.TYPE_NULL)
        elif isinstance(value, basestring):
            value = value.encode("utf-8")
            size = pack(">I", len(value))
            self._connection.write("".join([Types.TYPE_STRING, size, value]))
        elif isinstance(value, bool):
            data = pack(">?", value)
            self._connection.write("".join([Types.TYPE_BOOLEAN, data]))
        elif isinstance(value, (int, long)):
            data = pack(">q", value)
            self._connection.write("".join([Types.TYPE_LONG, data]))
        elif isinstance(value, float):
            data = pack(">d", value)
            self._connection.write("".join([Types.TYPE_DOUBLE, data]))
        else:
            raise Exception("Unsupported Type encountered.")

    def _collect3(self, value, consider_int=True):
        if not isinstance(value, (list, tuple)):
            self._send_field3(value)
        else:
            self._connection.write(Types.TYPE_TUPLE)
            meta = pack(">I", len(value))
            self._connection.write(bytes([meta[3]]))
            for field in value:
               self._collect3(field)

    def _send_field3(self, value):
        if value is None:
            self._connection.write(Types.TYPE_NULL)
        elif isinstance(value, bytes):
            size = pack(">I", len(value))
            self._connection.write(b"".join([Types.TYPE_BYTES, size, value]))
        elif isinstance(value, str):
            value = value.encode("utf-8")
            size = pack(">I", len(value))
            self._connection.write(b"".join([Types.TYPE_STRING, size, value]))
        elif isinstance(value, bool):
            data = pack(">?", value)
            self._connection.write(b"".join([Types.TYPE_BOOLEAN, data]))
        elif isinstance(value, int):
            data = pack(">q", value)
            self._connection.write(b"".join([Types.TYPE_LONG, data]))
        elif isinstance(value, float):
            data = pack(">d", value)
            self._connection.write(b"".join([Types.TYPE_DOUBLE, data]))
        else:
            raise Exception("Unsupported Type encountered.")