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


class GroupReduceFunction(Function.Function):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(GroupReduceFunction, self).__init__()

    def run(self):
        while self._iterator.next() is not None:
            self.group_reduce(self._iterator, self._collector)
            self._collector._finish()
            while self._iterator.has_next():
                self._iterator.next()
            self._iterator._reset()

    @abstractmethod
    def group_reduce(self, iterator, collector):
        pass