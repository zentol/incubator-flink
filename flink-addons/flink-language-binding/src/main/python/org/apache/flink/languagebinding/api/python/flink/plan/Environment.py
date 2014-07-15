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
from flink.connection import Connection
from flink.connection import Collector
from flink.plan.DataSet import DataSet
from flink.plan.Constants import _Fields, _Operations
from flink.utilities.Switch import Switch
from flink.plan.InputFormat import InputIdentifier, CSVInputFormat, TextInputFormat, ValueInputFormat
from flink.plan.OutputFormat import OutputIdentifier


def get_environment():
    """
    Creates an execution environment that represents the context in which the program is currently executed.
    
    :return:The execution environment of the context in which the program is executed.
    """
    return Environment()


class Environment(object):
    def __init__(self):
        # util
        self._connection = Connection.STDPipeConnection()
        self._collector = Collector.RawCollector(self._connection)
        self._counter = 0

        #parameters
        self._parameters = []

        #sets
        self._sources = []
        self._sets = []
        self._sinks = []

        #specials
        self._broadcast = []

    def read_csv(self, path, types, line_delimiter="\n", field_delimiter='|'):
        """
        Create a DataSet that represents the tuples produced by reading the given CSV file.

        :param path: The path of the CSV file.
        :param types: Specifies the types for the CSV fields.
        :return:A CsvReader that can be used to configure the CSV input.
        """
        return self._create_input(CSVInputFormat(path, types, line_delimiter, field_delimiter))

    def read_text(self, path):
        """
        Creates a DataSet that represents the Strings produced by reading the given file line wise.

        The file will be read with the system's default character set.

        :param path: The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
        :return: A DataSet that represents the data read from the given file as text lines.
        """
        return self._create_input(TextInputFormat(path))

    def from_elements(self, *elements):
        """
        Creates a new data set that contains the given elements.

        The elements must all be of the same type, for example, all of the String or Integer.
        The sequence of elements must not be empty.

        :param elements: The elements to make up the data set.
        :return: A DataSet representing the given list of elements.
        """
        return self._create_input(ValueInputFormat(elements))

    def _create_input(self, format):
        new_set = DataSet(self)
        dic = dict()
        dic[_Fields.FORMAT] = format
        dic[_Fields.CHILD] = new_set
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
        self._send_plan()

    def _send_plan(self):
        self._send_parameters()
        self._send_sources()
        self._send_sets()
        self._send_sinks()
        self._send_broadcast()

    def _send_parameters(self):
        self._collector.collect(len(self._parameters))
        for parameter in self._parameters:
            self._collector.collect(parameter)
        self._collector._finish()

    def _send_sources(self):
        self._collector.collect(len(self._sources))
        for source in self._sources:
            format = source[_Fields.FORMAT]
            set = source[_Fields.CHILD]
            self._collector.collect(format._identifier)
            self._collector.collect(set._id)
            for case in Switch(format._identifier):
                if case(InputIdentifier.VALUE):
                    self._collector.collect(len(format._arguments))
                    for value in format._arguments:
                        self._collector.collect(value)
                    break
                if case():
                    self._collector.collect(format._arguments)
                    break
        self._collector._finish()

    def _send_sets(self):
        self._collector.collect(len(self._sets))
        for set in self._sets:
            identifier = set.get(_Fields.IDENTIFIER)
            self._collector.collect(set[_Fields.IDENTIFIER])
            self._collector.collect(set[_Fields.PARENT]._id)
            self._collector.collect(set[_Fields.CHILD]._id)
            for case in Switch(identifier):
                if case(_Operations.SORT):
                    self._collector.collect(set[_Fields.FIELD])
                    self._collector.collect(set[_Fields.ORDER])
                    break
                if case(_Operations.GROUP):
                    self._collector.collect(set[_Fields.KEYS])
                    break
                if case(_Operations.COGROUP):
                    self._full(set)
                    break
                if case(_Operations.CROSS):
                    self._full_without_keys_prj(set)
                    break
                if case(_Operations.CROSSH):
                    self._full_without_keys_prj(set)
                    break
                if case(_Operations.CROSST):
                    self._full_without_keys_prj(set)
                    break
                if case(_Operations.FLATMAP):
                    self._operation_with_types(set)
                    break
                if case(_Operations.FILTER):
                    self._operation(set)
                    break
                if case(_Operations.GROUPREDUCE):
                    self._operation_with_types(set)
                    break
                if case(_Operations.JOIN):
                    self._full_prj(set)
                    break
                if case(_Operations.JOINH):
                    self._full_prj(set)
                    break
                if case(_Operations.JOINT):
                    self._full_prj(set)
                    break
                if case(_Operations.MAP):
                    self._operation_with_types(set)
                    break
                if case(_Operations.REDUCE):
                    self._operation(set)
                    break
                if case(_Operations.UNION):
                    self._collector.collect(set[_Fields.OTHER]._id)
                    break
                if case(_Operations.PROJECTION):
                    self._collector.collect(set[_Fields.KEYS])
                    self._collector.collect(set[_Fields.TYPES])
                    break
                if case():
                    raise KeyError("Environment._send_child_sets(): Invalid operation identifier: " + str(identifier))
        self._collector._finish()

    def _send_sinks(self):
        self._collector.collect(len(self._sinks))
        for sink in self._sinks:
            format = sink[_Fields.FORMAT]
            for case in Switch(format._identifier):
                if case(OutputIdentifier.PRINT):
                    set = sink[_Fields.SET]
                    self._collector.collect(set._id)
                    self._collector.collect(format._identifier)
                    break
                if case():
                    set = sink[_Fields.SET]
                    self._collector.collect(set._id)
                    self._collector.collect(format._identifier)
                    self._collector.collect(format._arguments)
                    break
        self._collector._finish()

    def _send_broadcast(self):
        self._collector.collect(len(self._broadcast))
        for entry in self._broadcast:
            set = entry[_Fields.SET]
            other = entry[_Fields.OTHER]
            name = entry[_Fields.NAME]
            self._collector.collect(set._id)
            self._collector.collect(other._id)
            self._collector.collect(name)
        self._collector._finish()

    def _full_prj(self, child):
        self._full(child)
        self._collector.collect(child[_Fields.PKEY1])
        self._collector.collect(child[_Fields.PKEY2])

    def _full(self, child):
        self._collector.collect(child[_Fields.KEY1])
        self._collector.collect(child[_Fields.KEY2])
        self._full_without_keys(child)

    def _full_without_keys_prj(self, child):
        self._full_without_keys(child)
        self._collector.collect(child[_Fields.PKEY1])
        self._collector.collect(child[_Fields.PKEY2])

    def _full_without_keys(self, child):
        self._collector.collect(child[_Fields.OTHER]._id)
        self._operation_with_types(child)

    def _operation_with_types(self, child):
        self._collector.collect(child[_Fields.TYPES])
        self._operation(child)

    def _operation(self, child):
        self._collector.collect(child[_Fields.OPERATOR])
        self._collector.collect(child[_Fields.META])