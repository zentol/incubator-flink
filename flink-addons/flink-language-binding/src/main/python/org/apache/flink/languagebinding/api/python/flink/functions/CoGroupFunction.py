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

from flink.functions import Function
from flink.connection import Iterator


class CoGroupFunction(Function.Function):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(CoGroupFunction, self).__init__()
        self.dummy1 = Iterator.Dummy(self._iterator, 0)
        self.dummy2 = Iterator.Dummy(self._iterator, 1)

    def run(self):
        while self._iterator.next() is not None:
            sig1 = self.dummy1.next()
            sig2 = self.dummy2.next()
            self.co_group(self.dummy1, self.dummy2, self._collector)
            self._collector._finish()
            self._iterator._reset()

    @abstractmethod
    def co_group(self, iterator1, iterator2, collector):
        pass