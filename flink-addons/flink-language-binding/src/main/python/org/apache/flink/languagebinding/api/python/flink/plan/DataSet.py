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
import inspect, copy, types as TYPES

from flink.plan.Constants import _Fields, _Identifier, WriteMode
from flink.functions.CoGroupFunction import CoGroupFunction
from flink.functions.FilterFunction import FilterFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.CrossFunction import CrossFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.JoinFunction import JoinFunction
from flink.functions.MapFunction import MapFunction
from flink.functions.MapPartitionFunction import MapPartitionFunction
from flink.functions.ReduceFunction import ReduceFunction


class Set(object):
    def __init__(self, env, dic):
        self._env = env
        self._id = env._counter
        env._counter += 1
        self._dic = dic
        self._dic[_Fields.BCVARS] = []
        self._dic[_Fields.CHILDREN] = []
        self._dic[_Fields.SINKS] = []
        self._dic[_Fields.NAME] = None

    def output(self):
        """
        Writes a DataSet to the standard output stream (stdout).
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Identifier.SINK_PRINT
        dic[_Fields.SET] = self
        self._dic[_Fields.SINKS].append(dic)
        self._env._sinks.append(dic)

    def write_text(self, path, write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a DataSet as a text file to the specified location.

        :param path: he path pointing to the location the text file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Identifier.SINK_TEXT
        dic[_Fields.SET] = self
        dic[_Fields.PATH] = path
        dic[_Fields.WRITE_MODE] = write_mode
        self._dic[_Fields.SINKS].append(dic)
        self._env._sinks.append(dic)

    def write_csv(self, path, line_delimiter="\n", field_delimiter=',', write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a Tuple DataSet as a CSV file to the specified location.

        Note: Only a Tuple DataSet can written as a CSV file.
        :param path: The path pointing to the location the CSV file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Identifier.SINK_CSV
        dic[_Fields.PATH] = path
        dic[_Fields.SET] = self
        dic[_Fields.DELIMITER_FIELD] = field_delimiter
        dic[_Fields.DELIMITER_LINE] = line_delimiter
        dic[_Fields.WRITE_MODE] = write_mode
        self._dic[_Fields.SINKS].append(dic)
        self._env._sinks.append(dic)

    def reduce_group(self, operator, types, combinable=False):
        """
        Applies a GroupReduce transformation.

        The transformation calls a GroupReduceFunction once for each group of the DataSet, or one when applied on a
        non-grouped DataSet.
        The GroupReduceFunction can iterate over all elements of the DataSet and
        emit any number of output elements including none.

        :param operator: The GroupReduceFunction that is applied on the DataSet.
        :param types: The type of the resulting DataSet.
        :return:A GroupReduceOperator that represents the reduced DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = GroupReduceFunction()
            operator.reduce = f
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        reduce = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.GROUPREDUCE
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = reduce
        operator._combine = False
        dic[_Fields.OPERATOR] = copy.deepcopy(operator)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        dic[_Fields.COMBINE] = combinable
        operator._combine = True
        dic[_Fields.COMBINEOP] = operator
        dic[_Fields.NAME] = "PythonGroupReduce"
        self._env._sets.append(dic)

        return reduce


class ReduceSet(Set):
    def __init__(self, env, dic):
        super(ReduceSet, self).__init__(env, dic)
        self._is_chained = False

    def reduce(self, operator):
        """
        Applies a Reduce transformation on a non-grouped DataSet.

        The transformation consecutively calls a ReduceFunction until only a single element remains which is the result
        of the transformation. A ReduceFunction combines two elements into one new element of the same type.

        :param operator:The ReduceFunction that is applied on the DataSet.
        :return:A ReduceOperator that represents the reduced DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = ReduceFunction()
            operator.reduce = f
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.REDUCE
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.OPERATOR] = operator
        dic[_Fields.COMBINEOP] = operator
        dic[_Fields.COMBINE] = False
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.NAME] = "PythonReduce"
        self._env._sets.append(dic)
        return new_set


class DataSet(ReduceSet):
    def __init__(self, env, dic):
        super(DataSet, self).__init__(env, dic)

    def project(self, *fields):
        """
        Applies a Project transformation on a Tuple DataSet.

        Note: Only Tuple DataSets can be projected. The transformation projects each Tuple of the DataSet onto a
        (sub)set of fields.

        :param fields: The field indexes of the input tuples that are retained.
                        The order of fields in the output tuple corresponds to the order of field indexes.
        :return: The projected DataSet.

        """
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = DataSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.PROJECTION
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.KEYS] = fields
        self._env._sets.append(dic)
        return new_set

    def group_by(self, *keys):
        """
        Groups a Tuple DataSet using field position keys.
        Note: Field position keys only be specified for Tuple DataSets.
        The field position keys specify the fields of Tuples on which the DataSet is grouped.
        This method returns an UnsortedGrouping on which one of the following grouping transformation can be applied.
        sort_group() to get a SortedGrouping.
        reduce() to apply a Reduce transformation.
        group_reduce() to apply a GroupReduce transformation.

        :param keys: One or more field positions on which the DataSet will be grouped.
        :return:A Grouping on which a transformation needs to be applied to obtain a transformed DataSet.
        """
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        dics = []
        new_set = UnsortedGrouping(self._env, dic, dics)
        dic[_Fields.IDENTIFIER] = _Identifier.GROUP
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.KEYS] = keys
        dics.append(dic)
        return new_set

    def co_group(self, other_set):
        """
        Initiates a CoGroup transformation which combines the elements of two DataSets into on DataSet.

        It groups each DataSet individually on a key and gives groups of both DataSets with equal keys together into a
        CoGroupFunction. If a DataSet has a group with no matching key in the other DataSet,
        the CoGroupFunction is called with an empty group for the non-existing group.
        The CoGroupFunction can iterate over the elements of both groups and return any number of elements
        including none.

        :param other_set: The other DataSet of the CoGroup transformation.
        :return:A CoGroupOperator to continue the definition of the CoGroup transformation.
        """
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = CoGroupOperatorWhere(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.COGROUP
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return new_set

    def cross(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        return self._cross(other_set, _Identifier.CROSS)

    def cross_with_huge(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.
        This method also gives the hint to the optimizer that
        the second DataSet to cross is much larger than the first one.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        return self._cross(other_set, _Identifier.CROSSH)

    def cross_with_tiny(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.
        This method also gives the hint to the optimizer that
        the second DataSet to cross is much smaller than the first one.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        return self._cross(other_set, _Identifier.CROSST)

    def _cross(self, other_set, identifier):
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = CrossOperator(self._env, dic)
        dic[_Fields.IDENTIFIER] = identifier
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.OTHER] = other_set
        dic[_Fields.PROJECTIONS] = []
        dic[_Fields.OPERATOR] = None
        dic[_Fields.META] = None
        self._env._sets.append(dic)
        return new_set

    def filter(self, operator):
        """
        Applies a Filter transformation on a DataSet.

        he transformation calls a FilterFunction for each element of the DataSet and retains only those element
        for which the function returns true. Elements for which the function returns false are filtered.

        :param operator: The FilterFunction that is called for each element of the DataSet.
        :return:A FilterOperator that represents the filtered DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = FilterFunction()
            operator.filter = f
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.FILTER
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.OPERATOR] = operator
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.NAME] = "PythonFilter"
        self._env._sets.append(dic)
        return new_set

    def flat_map(self, operator, types):
        """
        Applies a FlatMap transformation on a DataSet.

        The transformation calls a FlatMapFunction for each element of the DataSet.
        Each FlatMapFunction call can return any number of elements including none.

        :param operator: The FlatMapFunction that is called for each element of the DataSet.
        :param types: The type of the resulting DataSet.
        :return:A FlatMapOperator that represents the transformed DataSe
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = FlatMapFunction()
            operator.flat_map = f
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.FLATMAP
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.OPERATOR] = operator
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        dic[_Fields.NAME] = "PythonFlatMap"
        self._env._sets.append(dic)
        return new_set

    def join(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        return self._join(other_set, _Identifier.JOIN)

    def join_with_huge(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.
        This method also gives the hint to the optimizer that
        the second DataSet to join is much larger than the first one.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        return self._join(other_set, _Identifier.JOINH)

    def join_with_tiny(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.
        This method also gives the hint to the optimizer that
        the second DataSet to join is much smaller than the first one.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        return self._join(other_set, _Identifier.JOINT)

    def _join(self, other_set, identifier):
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = JoinOperatorWhere(self._env, dic)
        dic[_Fields.IDENTIFIER] = identifier
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        dic[_Fields.OPERATOR] = None
        dic[_Fields.META] = None
        dic[_Fields.PROJECTIONS] = []
        self._env._sets.append(dic)
        return new_set

    def map(self, operator, types):
        """
        Applies a Map transformation on a DataSet.

        The transformation calls a MapFunction for each element of the DataSet.
        Each MapFunction call returns exactly one element.

        :param operator: The MapFunction that is called for each element of the DataSet.
        :param types: The type of the resulting DataSet
        :return:A MapOperator that represents the transformed DataSet
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = MapFunction()
            operator.map = f
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.MAP
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.OPERATOR] = operator
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        dic[_Fields.NAME] = "PythonMap"
        self._env._sets.append(dic)
        return new_set

    def map_partition(self, operator, types):
        """
        Applies a MapPartition transformation on a DataSet.

        The transformation calls a MapPartitionFunction once per parallel partition of the DataSet.
        The entire partition is available through the given Iterator.
        Each MapPartitionFunction may return an arbitrary number of results.

        The number of elements that each instance of the MapPartition function
        sees is non deterministic and depends on the degree of parallelism of the operation.

        :param operator: The MapFunction that is called for each element of the DataSet.
        :param types: The type of the resulting DataSet
        :return:A MapOperator that represents the transformed DataSet
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = MapPartitionFunction()
            operator.map_partition = f
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.MAPPARTITION
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.OPERATOR] = operator
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        dic[_Fields.NAME] = "PythonMapPartition"
        self._env._sets.append(dic)
        return new_set

    def union(self, other_set):
        """
        Creates a union of this DataSet with an other DataSet.

        The other DataSet must be of the same data type.

        :param other_set: The other DataSet which is unioned with the current DataSet.
        :return:The resulting DataSet.
        """
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = DataSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.UNION
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.OTHER] = other_set
        self._env._sets.append(dic)
        return new_set


class OperatorSet(DataSet):
    def __init__(self, env, dic):
        super(OperatorSet, self).__init__(env, dic)

    def with_broadcast_set(self, name, set):
        dic = dict()
        set._dic[_Fields.CHILDREN].append(dic)
        dic[_Fields.SET] = self
        dic[_Fields.OTHER] = set
        dic[_Fields.NAME] = name
        self._env._broadcast.append(dic)
        self._dic[_Fields.BCVARS].append(dic)

        return self


class Grouping(object):
    def __init__(self, env, dic, dics):
        self._env = env
        self._dics = dics
        self._id = env._counter
        env._counter += 1
        self._dic = dic
        dic[_Fields.CHILDREN] = []
        dic[_Fields.SINKS] = []

    def reduce_group(self, operator, types, combinable=False):
        """
        Applies a GroupReduce transformation.

        The transformation calls a GroupReduceFunction once for each group of the DataSet, or one when applied on a
        non-grouped DataSet.
        The GroupReduceFunction can iterate over all elements of the DataSet and
        emit any number of output elements including none.

        :param operator: The GroupReduceFunction that is applied on the DataSet.
        :param types: The type of the resulting DataSet.
        :return:A GroupReduceOperator that represents the reduced DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = GroupReduceFunction()
            operator.reduce = f
        operator._set_keys(self._dics[0][_Fields.KEYS])
        sort_ops = []
        for x in range(len(self._dics) - 1):
            sort_ops.append((self._dics[x + 1][_Fields.FIELD], self._dics[x + 1][_Fields.ORDER]))
        operator._set_sort_ops(sort_ops)
        for i in self._dics:
            self._env._sets.append(i)
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        reduce = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.GROUPREDUCE
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = reduce
        operator._combine = False
        dic[_Fields.OPERATOR] = copy.deepcopy(operator)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        dic[_Fields.COMBINE] = combinable
        operator._combine = True
        dic[_Fields.COMBINEOP] = operator
        dic[_Fields.NAME] = "PythonGroupReduce"
        self._env._sets.append(dic)

        return reduce

    def sort_group(self, field, order):
        """
        Sorts Tuple elements within a group on the specified field in the specified Order.

        Note: Only groups of Tuple elements can be sorted.
        Groups can be sorted by multiple fields by chaining sort_group() calls.

        :param field:The Tuple field on which the group is sorted.
        :param order: The Order in which the specified Tuple field is sorted. See DataSet.Order.
        :return:A SortedGrouping with specified order of group element.
        """
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        new_set = SortedGrouping(self._env, dic, self._dics)
        dic[_Fields.IDENTIFIER] = _Identifier.SORT
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = new_set
        dic[_Fields.FIELD] = field
        dic[_Fields.ORDER] = order
        self._dics.append(dic)
        return new_set


class UnsortedGrouping(Grouping):
    def __init__(self, env, dic, dics):
        super(UnsortedGrouping, self).__init__(env, dic, dics)

    def reduce(self, operator):
        """
        Applies a Reduce transformation on a non-grouped DataSet.

        The transformation consecutively calls a ReduceFunction until only a single element remains which is the result
        of the transformation. A ReduceFunction combines two elements into one new element of the same type.

        :param operator:The ReduceFunction that is applied on the DataSet.
        :return:A ReduceOperator that represents the reduced DataSet.
        """
        operator._set_keys(self._dics[0][_Fields.KEYS])
        for i in self._dics:
            self._env._sets.append(i)
        dic = dict()
        self._dic[_Fields.CHILDREN].append(dic)
        reduce = OperatorSet(self._env, dic)
        dic[_Fields.IDENTIFIER] = _Identifier.REDUCE
        dic[_Fields.PARENT] = self
        dic[_Fields.SET] = reduce
        operator._combine = False
        dic[_Fields.OPERATOR] = copy.deepcopy(operator)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.COMBINE] = True
        operator._combine = True
        dic[_Fields.COMBINEOP] = operator
        dic[_Fields.NAME] = "PythonReduce"
        self._env._sets.append(dic)

        return reduce


class SortedGrouping(Grouping):
    def __init__(self, env, dic, dics):
        super(SortedGrouping, self).__init__(env, dic, dics)


class CoGroupOperatorWhere(object):
    def __init__(self, env, dic):
        self._env = env
        self._dic = dic

    def where(self, *fields):
        """
        Continues a CoGroup transformation.

        Defines the Tuple fields of the first co-grouped DataSet that should be used as grouping keys.
        Note: Fields can only be selected as grouping keys on Tuple DataSets.

        :param fields: The indexes of the Tuple fields of the first co-grouped DataSets that should be used as keys.
        :return: An incomplete CoGroup transformation.
        """
        self._dic[_Fields.KEY1] = fields
        return CoGroupOperatorTo(self._env, self._dic)


class CoGroupOperatorTo(object):
    def __init__(self, env, dic):
        self._env = env
        self._dic = dic

    def equal_to(self, *fields):
        """
        Continues a CoGroup transformation.

        Defines the Tuple fields of the second co-grouped DataSet that should be used as grouping keys.
        Note: Fields can only be selected as grouping keys on Tuple DataSets.

        :param fields: The indexes of the Tuple fields of the second co-grouped DataSet that should be used as keys.
        :return: An incomplete CoGroup transformation.
        """
        self._dic[_Fields.KEY2] = fields
        return CoGroupOperatorUsing(self._env, self._dic)


class CoGroupOperatorUsing(object):
    def __init__(self, env, dic):
        self._env = env
        self._dic = dic

    def using(self, operator, types):
        """
        Finalizes a CoGroup transformation.

        Applies a CoGroupFunction to groups of elements with identical keys.
        Each CoGroupFunction call returns an arbitrary number of keys.

        :param operator: The CoGroupFunction that is called for all groups of elements with identical keys.
        :param types: The type of the resulting DataSet.
        :return:An CoGroupOperator that represents the co-grouped result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = CoGroupFunction()
            operator.co_group = f
        new_set = OperatorSet(self._env, self._dic)
        operator._keys1 = self._dic[_Fields.KEY1]
        operator._keys2 = self._dic[_Fields.KEY2]
        self._dic[_Fields.OPERATOR] = operator
        self._dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._dic[_Fields.TYPES] = types
        self._dic[_Fields.SET] = new_set
        self._dic[_Fields.NAME] = "PythonCoGroup"
        self._env._sets.append(self._dic)
        return new_set


class JoinOperatorWhere(object):
    def __init__(self, env, dic):
        self._env = env
        self._dic = dic

    def where(self, *fields):
        """
        Continues a Join transformation.

        Defines the Tuple fields of the first join DataSet that should be used as join keys.
        Note: Fields can only be selected as join keys on Tuple DataSets.

        :param fields: The indexes of the Tuple fields of the first join DataSets that should be used as keys.
        :return:An incomplete Join transformation.

        """
        self._dic[_Fields.KEY1] = fields
        return JoinOperatorTo(self._env, self._dic)


class JoinOperatorTo(object):
    def __init__(self, env, dic):
        self._env = env
        self._dic = dic

    def equal_to(self, *fields):
        """
        Continues a Join transformation.

        Defines the Tuple fields of the second join DataSet that should be used as join keys.
        Note: Fields can only be selected as join keys on Tuple DataSets.

        :param fields:The indexes of the Tuple fields of the second join DataSet that should be used as keys.
        :return:An incomplete Join Transformation.
        """
        self._dic[_Fields.KEY2] = fields
        return JoinOperator(self._env, self._dic)


class JoinOperatorProjection(DataSet):
    def __init__(self, env, dic, id):
        super(JoinOperatorProjection, self).__init__(env, dic)
        self._id = id

    def project_first(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("first", fields))
        return self

    def project_second(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("second", fields))
        return self


class JoinOperator(DataSet):
    def __init__(self, env, dic):
        super(JoinOperator, self).__init__(env, dic)
        self._dic[_Fields.TYPES] = None
        self._dic[_Fields.SET] = self

    def project_first(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("first", fields))
        return JoinOperatorProjection(self._env, self._dic, self._id)

    def project_second(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("second", fields))
        return JoinOperatorProjection(self._env, self._dic, self._id)

    def using(self, operator, types):
        """
        Finalizes a Join transformation.

        Applies a JoinFunction to each pair of joined elements. Each JoinFunction call returns exactly one element.

        :param operator:The JoinFunction that is called for each pair of joined elements.
        :param types:
        :return:An Set that represents the joined result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = JoinFunction()
            operator.join = f
        self._dic[_Fields.OPERATOR] = operator
        self._dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._dic[_Fields.TYPES] = types
        self._dic[_Fields.NAME] = "PythonJoin"
        self._env._sets.append(self._dic)
        return self


class CrossOperatorProjection(DataSet):
    def __init__(self, env, dic, id):
        super(CrossOperatorProjection, self).__init__(env, dic)
        self._id = id

    def project_first(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("first", fields))
        return self

    def project_second(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("second", fields))
        return self


class CrossOperator(DataSet):
    def __init__(self, env, dic):
        super(CrossOperator, self).__init__(env, dic)
        dic[_Fields.TYPES] = None

    def project_first(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("first", fields))
        return CrossOperatorProjection(self._env, self._dic, self._id)

    def project_second(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PROJECTIONS].append(("second", fields))
        return CrossOperatorProjection(self._env, self._dic, self._id)

    def using(self, operator, types):
        """
        Finalizes a Cross transformation.

        Applies a CrossFunction to each pair of joined elements. Each CrossFunction call returns exactly one element.

        :param operator:The CrossFunction that is called for each pair of joined elements.
        :param types: The type of the resulting DataSet.
        :return:An Set that represents the joined result DataSet.
        """
        if isinstance(operator, TYPES.FunctionType):
            f = operator
            operator = CrossFunction()
            operator.cross = f
        self._dic[_Fields.OPERATOR] = operator
        self._dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._dic[_Fields.TYPES] = types
        self._dic[_Fields.NAME] = "PythonCross"
        return self
