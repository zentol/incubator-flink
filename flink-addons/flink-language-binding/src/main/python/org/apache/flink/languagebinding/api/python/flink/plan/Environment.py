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
import inspect

from flink.connection import Connection
from flink.connection import Collector
from flink.plan.DataSet import DataSet
from flink.plan.Constants import _Fields, _Identifier
from flink.utilities.Switch import Switch
import dill


def get_environment():
    """
    Creates an execution environment that represents the context in which the program is currently executed.
    
    :return:The execution environment of the context in which the program is executed.
    """
    return Environment()


class Environment(object):
    def __init__(self):
        # util
        self._connection = Connection.OneWayBusyBufferingMappedFileConnection()
        self._collector = Collector.TypedCollector(self._connection)
        self._counter = 0

        #parameters
        self._parameters = []

        #sets
        self._sources = []
        self._sets = []
        self._sinks = []

        #specials
        self._broadcast = []

    def read_csv(self, path, types, line_delimiter="\n", field_delimiter=','):
        """
        Create a DataSet that represents the tuples produced by reading the given CSV file.

        :param path: The path of the CSV file.
        :param types: Specifies the types for the CSV fields.
        :return:A CsvReader that can be used to configure the CSV input.
        """
        dic = dict()
        new_set = DataSet(self, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.SOURCE_CSV
        dic[_Fields.DELIMITER_LINE] = line_delimiter
        dic[_Fields.DELIMITER_FIELD] = field_delimiter
        dic[_Fields.PATH] = path
        dic[_Fields.TYPES] = types
        dic[_Fields.SET] = new_set
        self._sources.append(dic)
        return new_set

    def read_text(self, path):
        """
        Creates a DataSet that represents the Strings produced by reading the given file line wise.

        The file will be read with the system's default character set.

        :param path: The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
        :return: A DataSet that represents the data read from the given file as text lines.
        """
        dic = dict()
        new_set = DataSet(self, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.SOURCE_TEXT
        dic[_Fields.PATH] = path
        dic[_Fields.SET] = new_set
        self._sources.append(dic)
        return new_set

    def from_elements(self, *elements):
        """
        Creates a new data set that contains the given elements.

        The elements must all be of the same type, for example, all of the String or Integer.
        The sequence of elements must not be empty.

        :param elements: The elements to make up the data set.
        :return: A DataSet representing the given list of elements.
        """
        dic = dict()
        new_set = DataSet(self, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.SOURCE_VALUE
        dic[_Fields.VALUES] = elements
        dic[_Fields.SET] = new_set
        self._sources.append(dic)
        return new_set

    def set_degree_of_parallelism(self, degree):
        """
        Sets the degree of parallelism (DOP) for operations executed through this environment.

        Setting a DOP of x here will cause all operators (such as join, map, reduce) to run with x parallel instances.

        :param degreeOfParallelism: The degree of parallelism
        """
        self._parameters.append(("dop", degree))

    def execute(self, local=False):
        """
        Triggers the program execution.

        The environment will execute all parts of the program that have resulted in a "sink" operation.
        """
        self._parameters.append(("mode", local))
        self._optimize_plan()
        self._send_plan()
        self._connection._write_buffer()

    def _optimize_plan(self):
        self._find_chains()

    def _find_chains(self):
        udf = set([_Identifier.MAP, _Identifier.FLATMAP, _Identifier.FILTER, _Identifier.MAPPARTITION,
                   _Identifier.GROUPREDUCE, _Identifier.REDUCE, _Identifier.COGROUP,
                   _Identifier.CROSS, _Identifier.CROSSH, _Identifier.CROSST,
                   _Identifier.JOIN, _Identifier.JOINH, _Identifier.JOINT])
        sink = set([_Identifier.SINK_CSV, _Identifier.SINK_TEXT, _Identifier.SINK_PRINT])
        chainable = set([_Identifier.MAP, _Identifier.FILTER, _Identifier.FLATMAP, _Identifier.GROUPREDUCE, _Identifier.REDUCE])
        x = len(self._sets) - 1
        while x > -1:
            operator_dictionary = self._sets[x]
            operator_type = operator_dictionary[_Fields.IDENTIFIER]
            if operator_type in chainable:
                parent_dictionary = operator_dictionary[_Fields.PARENT]._dic
                parent_type = parent_dictionary[_Fields.IDENTIFIER]
                if len(parent_dictionary[_Fields.SINKS])==0:
                    if operator_type == _Identifier.GROUPREDUCE or operator_type == _Identifier.REDUCE:
                        if operator_dictionary[_Fields.COMBINE]:
                            while parent_type == _Identifier.GROUP or parent_type == _Identifier.SORT:
                                parent_dictionary = parent_dictionary[_Fields.PARENT]._dic
                                parent_type = parent_dictionary[_Fields.IDENTIFIER]
                            if parent_type in udf and len(parent_dictionary[_Fields.CHILDREN]) == 1:
                                parent_operator = parent_dictionary[_Fields.OPERATOR]
                                function = operator_dictionary[_Fields.COMBINEOP]
                                meta = str(inspect.getmodule(function)) + "|" + str(function.__class__.__name__)
                                parent_operator._chain(dill.dumps(function, protocol=0, byref=True), meta)
                                operator_dictionary[_Fields.COMBINE] = False
                                parent_dictionary[_Fields.NAME] += " -> PythonCombine"
                    else:
                        if parent_type in udf and len(parent_dictionary[_Fields.CHILDREN]) == 1:
                            parent_operator = parent_dictionary[_Fields.OPERATOR]
                            if parent_operator is not None:
                                function = operator_dictionary[_Fields.OPERATOR]
                                meta = str(inspect.getmodule(function)) + "|" + str(function.__class__.__name__)
                                parent_operator._chain(dill.dumps(function, protocol=0, byref=True), meta)
                                parent_dictionary[_Fields.NAME] += " -> " + operator_dictionary[_Fields.NAME]
                                for child in operator_dictionary[_Fields.CHILDREN]:
                                    child[_Fields.PARENT] = parent_dictionary[_Fields.SET]
                                    parent_dictionary[_Fields.CHILDREN].append(child[_Fields.SET])
                                for sink in operator_dictionary[_Fields.SINKS]:
                                    sink[_Fields.SET] = parent_dictionary[_Fields.SET]
                                for bcvar in operator_dictionary[_Fields.BCVARS]:
                                    bcvar[_Fields.SET] = parent_dictionary[_Fields.SET]
                                self._remove_set((operator_dictionary[_Fields.SET]))
            x -= 1

    def _remove_set(self, set):
        self._sets[:] = [s for s in self._sets if s[_Fields.SET]._id!=set._id]

    def _send_plan(self):
        self._send_parameters()
        self._collector.collect(len(self._sources) + len(self._sets) + len(self._sinks) + len(self._broadcast))
        self._send_sources()
        self._send_operations()
        self._send_sinks()
        self._send_broadcast()

    def _send_parameters(self):
        self._collector.collect(len(self._parameters))
        for parameter in self._parameters:
            self._collector.collect(parameter)

    def _send_sources(self):
        for source in self._sources:
            identifier = source[_Fields.IDENTIFIER]
            self._collector.collect(identifier)
            self._collector.collect(source[_Fields.SET]._id)
            for case in Switch(identifier):
                if case(_Identifier.SOURCE_CSV):
                    self._collector.collect(source[_Fields.PATH])
                    self._collector.collect(source[_Fields.DELIMITER_FIELD])
                    self._collector.collect(source[_Fields.DELIMITER_LINE])
                    self._collector.collect(source[_Fields.TYPES])
                    break
                if case(_Identifier.SOURCE_TEXT):
                    self._collector.collect(source[_Fields.PATH])
                    break
                if case(_Identifier.SOURCE_VALUE):
                    self._collector.collect(len(source[_Fields.VALUES]))
                    for value in source[_Fields.VALUES]:
                        self._collector.collect(value)
                    break

    def _send_operations(self):
        collect = self._collector.collect
        collectBytes = self._collector.collectBytes
        for set in self._sets:
            identifier = set.get(_Fields.IDENTIFIER)
            collect(set[_Fields.IDENTIFIER])
            collect(set[_Fields.PARENT]._id)
            collect(set[_Fields.SET]._id)
            for case in Switch(identifier):
                if case(_Identifier.SORT):
                    collect(set[_Fields.FIELD])
                    collect(set[_Fields.ORDER])
                    break
                if case(_Identifier.GROUP):
                    collect(set[_Fields.KEYS])
                    break
                if case(_Identifier.COGROUP):
                    collect(set[_Fields.OTHER]._id)
                    collect(set[_Fields.KEY1])
                    collect(set[_Fields.KEY2])
                    collectBytes(dill.dumps(set[_Fields.OPERATOR], protocol=0, byref=True))
                    collect(set[_Fields.META])
                    collect(set[_Fields.TYPES])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.CROSS) or case(_Identifier.CROSSH) or case(_Identifier.CROSST):
                    collect(set[_Fields.OTHER]._id)
                    collectBytes(dill.dumps(set[_Fields.OPERATOR], protocol=0, byref=True))
                    collect(set[_Fields.META])
                    collect(set[_Fields.TYPES])
                    collect(len(set[_Fields.PROJECTIONS]))
                    for p in set[_Fields.PROJECTIONS]:
                        collect(p[0])
                        collect(p[1])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.FILTER):
                    collectBytes(dill.dumps(set[_Fields.OPERATOR], protocol=0, byref=True))
                    collect(set[_Fields.META])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.GROUPREDUCE):
                    collectBytes(dill.dumps(set[_Fields.OPERATOR], protocol=0, byref=True))
                    collectBytes(dill.dumps(set[_Fields.COMBINEOP], protocol=0, byref=True))
                    collect(set[_Fields.META])
                    collect(set[_Fields.TYPES])
                    collect(set[_Fields.COMBINE])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.JOIN) or case(_Identifier.JOINH) or case(_Identifier.JOINT):
                    collect(set[_Fields.KEY1])
                    collect(set[_Fields.KEY2])
                    collect(set[_Fields.OTHER]._id)
                    collectBytes(dill.dumps(set[_Fields.OPERATOR], protocol=0, byref=True))
                    collect(set[_Fields.META])
                    collect(set[_Fields.TYPES])
                    collect(len(set[_Fields.PROJECTIONS]))
                    for p in set[_Fields.PROJECTIONS]:
                        collect(p[0])
                        collect(p[1])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.MAP) or case(_Identifier.MAPPARTITION) or case(_Identifier.FLATMAP):
                    collectBytes(dill.dumps(set[_Fields.OPERATOR], protocol=0, byref=True))
                    collect(set[_Fields.META])
                    collect(set[_Fields.TYPES])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.REDUCE):
                    collectBytes(dill.dumps(set[_Fields.OPERATOR], protocol=0, byref=True))
                    collectBytes(dill.dumps(set[_Fields.COMBINEOP], protocol=0, byref=True))
                    collect(set[_Fields.META])
                    collect(set[_Fields.COMBINE])
                    collect(set[_Fields.NAME])
                    break
                if case(_Identifier.UNION):
                    collect(set[_Fields.OTHER]._id)
                    break
                if case(_Identifier.PROJECTION):
                    collect(set[_Fields.KEYS])
                    break
                if case():
                    raise KeyError("Environment._send_child_sets(): Invalid operation identifier: " + str(identifier))

    def _send_sinks(self):
        for sink in self._sinks:
            identifier = sink[_Fields.IDENTIFIER]
            self._collector.collect(identifier)
            self._collector.collect(sink[_Fields.SET]._id)
            for case in Switch(identifier):
                if case(_Identifier.SINK_CSV):
                    self._collector.collect(sink[_Fields.PATH])
                    self._collector.collect(sink[_Fields.DELIMITER_FIELD])
                    self._collector.collect(sink[_Fields.DELIMITER_LINE])
                    self._collector.collect(sink[_Fields.WRITE_MODE])
                    break;
                if case(_Identifier.SINK_TEXT):
                    self._collector.collect(sink[_Fields.PATH])
                    self._collector.collect(sink[_Fields.WRITE_MODE])
                    break
                if case(_Identifier.SINK_PRINT):
                    break

    def _send_broadcast(self):
        for entry in self._broadcast:
            set = entry[_Fields.SET]
            other = entry[_Fields.OTHER]
            name = entry[_Fields.NAME]
            self._collector.collect(_Identifier.BROADCAST)
            self._collector.collect(set._id)
            self._collector.collect(other._id)
            self._collector.collect(name)