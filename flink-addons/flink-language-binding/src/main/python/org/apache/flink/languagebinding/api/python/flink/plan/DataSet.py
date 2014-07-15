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

from flink.plan.OutputFormat import CSVOutputFormat, PrintingOutputFormat, TextOutputFormat
from flink.plan.Constants import _Fields, _Operations, WriteMode


try:
    import cPickle as pickle
except:
    import pickle


class Set(object):
    def __init__(self, env):
        self._env = env
        self._id = env._counter
        env._counter += 1

    def output(self):
        """
        Writes a DataSet to the standard output stream (stdout).
        """
        self._create_sink(PrintingOutputFormat())

    def write_text(self, path, write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a DataSet as a text file to the specified location.

        :param path: he path pointing to the location the text file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        self._create_sink(TextOutputFormat(path, write_mode))

    def write_csv(self, path, line_delimiter="\n", field_delimiter='|', write_mode=WriteMode.NO_OVERWRITE):
        """
        Writes a Tuple DataSet as a CSV file to the specified location.

        Note: Only a Tuple DataSet can written as a CSV file.
        :param path: The path pointing to the location the CSV file is written to.
        :param write_mode: OutputFormat.WriteMode value, indicating whether files should be overwritten
        """
        self._create_sink(CSVOutputFormat(path, line_delimiter, field_delimiter, write_mode))

    def _create_sink(self, output_format):
        dic = dict()
        dic[_Fields.FORMAT] = output_format
        dic[_Fields.SET] = self
        self._env._sinks.append(dic)

    def groupreduce(self, operator, types):
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
        new_set = OperatorSet(self, self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.GROUPREDUCE
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        self._env._sets.append(dic)
        return new_set


class ReduceSet(Set):
    def __init__(self, env):
        super(ReduceSet, self).__init__(env)

    def reduce(self, operator):
        """
        Applies a Reduce transformation on a non-grouped DataSet.

        The transformation consecutively calls a ReduceFunction until only a single element remains which is the result
        of the transformation. A ReduceFunction combines two elements into one new element of the same type.

        :param operator:The ReduceFunction that is applied on the DataSet.
        :return:A ReduceOperator that represents the reduced DataSet.
        """
        new_set = OperatorSet(self, self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.REDUCE
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._env._sets.append(dic)
        return new_set


class DataSet(ReduceSet):
    def __init__(self, env):
        super(DataSet, self).__init__(env)

    def project(self, *fields):
        """
        Initiates a Project transformation on a Tuple DataSet.

        Note: Only Tuple DataSets can be projected. The transformation projects each Tuple of the DataSet onto a
        (sub)set of fields. This method returns a Projection on which Projection.types() needs to be called to
        completed the transformation.

        :param fields: The field indexes of the input tuples that are retained.
                        The order of fields in the output tuple corresponds to the order of field indexes.
        :return:A ProjectOperator on which types() needs to be called to complete the Project transformation.

        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.PROJECTION
        dic[_Fields.PARENT] = self
        dic[_Fields.KEYS] = fields
        return Projection(self, dic)

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
        new_set = UnsortedGrouping(self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.GROUP
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.KEYS] = keys
        self._env._sets.append(dic)
        return new_set

    def cogroup(self, other_set):
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
        dic[_Fields.IDENTIFIER] = _Operations.COGROUP
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return CoGroupOperatorWhere(self, dic)

    def cross(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.CROSS
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return CrossOperator(self, dic)

    def cross_with_huge(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.
        This method also gives the hint to the optimizer that
        the second DataSet to cross is much larger than the first one.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.CROSSH
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return CrossOperator(self, dic)

    def cross_with_tiny(self, other_set):
        """
        Initiates a Cross transformation which combines the elements of two DataSets into one DataSet.

        It builds all pair combinations of elements of both DataSets, i.e., it builds a Cartesian product.
        This method also gives the hint to the optimizer that
        the second DataSet to cross is much smaller than the first one.

        :param other_set: The other DataSet with which this DataSet is crossed.
        :return:A CrossOperator to continue the definition of the Cross transformation.
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.CROSST
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return CrossOperator(self, dic)

    def filter(self, operator):
        """
        Applies a Filter transformation on a DataSet.

        he transformation calls a FilterFunction for each element of the DataSet and retains only those element
        for which the function returns true. Elements for which the function returns false are filtered.

        :param operator: The FilterFunction that is called for each element of the DataSet.
        :return:A FilterOperator that represents the filtered DataSet.
        """
        new_set = OperatorSet(self, self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.FILTER
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._env._sets.append(dic)
        return new_set

    def flatmap(self, operator, types):
        """
        Applies a FlatMap transformation on a DataSet.

        The transformation calls a FlatMapFunction for each element of the DataSet.
        Each FlatMapFunction call can return any number of elements including none.

        :param operator: The FlatMapFunction that is called for each element of the DataSet.
        :param types: The type of the resulting DataSet.
        :return:A FlatMapOperator that represents the transformed DataSe
        """
        new_set = OperatorSet(self, self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.FLATMAP
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        self._env._sets.append(dic)
        return new_set

    def join(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.JOIN
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return JoinOperatorWhere(self, dic)

    def join_with_huge(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.
        This method also gives the hint to the optimizer that
        the second DataSet to join is much larger than the first one.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.JOINH
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return JoinOperatorWhere(self, dic)

    def join_with_tiny(self, other_set):
        """
        Initiates a Join transformation.

        A Join transformation joins the elements of two DataSets on key equality.
        This method also gives the hint to the optimizer that
        the second DataSet to join is much smaller than the first one.

        :param other_set: The other DataSet with which this DataSet is joined
        :return:A JoinOperator to continue the definition of the Join transformation.
        """
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.JOINT
        dic[_Fields.PARENT] = self
        dic[_Fields.OTHER] = other_set
        return JoinOperatorWhere(self, dic)

    def map(self, operator, types):
        """
        Applies a Map transformation on a DataSet.

        The transformation calls a MapFunction for each element of the DataSet.
        Each MapFunction call returns exactly one element.

        :param operator: The MapFunction that is called for each element of the DataSet.
        :param types: THe type of the resulting DataSet
        :return:A MapOperator that represents the transformed DataSet
        """
        new_set = OperatorSet(self, self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.MAP
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        dic[_Fields.TYPES] = types
        self._env._sets.append(dic)
        return new_set

    def union(self, other_set):
        """
        Creates a union of this DataSet with an other DataSet.

        The other DataSet must be of the same data type.

        :param other_set: The other DataSet which is unioned with the current DataSet.
        :return:The resulting DataSet.
        """
        new_set = DataSet(self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.UNION
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.OTHER] = other_set
        self._env._sets.append(dic)
        return new_set


class OperatorSet(DataSet):
    def __init__(self, ref, env):
        super(OperatorSet, self).__init__(env)
        self._ref = ref

        # def with_accumulator(self, name, acc):
        #    self._specials.append(("accumulator", name, acc))
        #    return self

    def with_broadcast_set(self, name, set):
        dic = dict()
        dic[_Fields.SET] = self._ref
        dic[_Fields.OTHER] = set
        dic[_Fields.NAME] = name
        self._env._broadcast.append(dic)
        return OperatorSet(self._ref, self._env)


class UnsortedGrouping(ReduceSet):
    def __init__(self, env):
        super(UnsortedGrouping, self).__init__(env)

    def sort_group(self, field, order):
        """
        Sorts Tuple elements within a group on the specified field in the specified Order.

        Note: Only groups of Tuple elements can be sorted.
        Groups can be sorted by multiple fields by chaining sort_group() calls.

        :param field:The Tuple field on which the group is sorted.
        :param order: The Order in which the specified Tuple field is sorted. See DataSet.Order.
        :return:A SortedGrouping with specified order of group element.
        """
        new_set = SortedGrouping(self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Operations.SORT
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.FIELD] = field
        dic[_Fields.ORDER] = order
        self._env._sets.append(dic)
        return new_set


class SortedGrouping(Set):
    def __init__(self, env):
        super(SortedGrouping, self).__init__(env)

    def sort_group(self, field, order):
        """
        Sorts Tuple elements within a group on the specified field in the specified Order.

        Note: Only groups of Tuple elements can be sorted.
        Groups can be sorted by multiple fields by chaining sort_group() calls.

        :param field:The Tuple field on which the group is sorted.
        :param order: The Order in which the specified Tuple field is sorted. See DataSet.Order.
        :return:A SortedGrouping with specified order of group element.
        """
        new_set = SortedGrouping(self._env)
        dic = dict()
        dic[_Fields.IDENTIFIER] = _Fields.SORT
        dic[_Fields.PARENT] = self
        dic[_Fields.CHILD] = new_set
        dic[_Fields.FIELD] = field
        dic[_Fields.ORDER] = order
        self._env._sets.append(dic)
        return new_set


class CoGroupOperatorWhere(object):
    def __init__(self, ref, dic):
        self._ref = ref
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
        return CoGroupOperatorTo(self._ref, self._dic)


class CoGroupOperatorTo(object):
    def __init__(self, ref, dic):
        self._ref = ref
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
        return CoGroupOperatorUsing(self._ref, self._dic)


class CoGroupOperatorUsing(object):
    def __init__(self, ref, dic):
        self._ref = ref
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
        new_set = OperatorSet(self._dic[_Fields.PARENT], self._ref._env)
        self._dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        self._dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._dic[_Fields.TYPES] = types
        self._dic[_Fields.CHILD] = new_set
        self._ref._env._sets.append(self._dic)
        return new_set


class Projection(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def types(self, *types):
        """
        Finalizes a Project Transformation.

        :param types: The type of the resulting DataSet
        :return: The projected DataSet.
        """
        new_set = DataSet(self._ref._env)
        self._dic[_Fields.CHILD] = new_set
        self._dic[_Fields.TYPES] = types
        self._ref._env._sets.append(self._dic)
        return new_set


class JoinOperatorWhere(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic
        self._dic[_Fields.OPERATOR] = None
        self._dic[_Fields.META] = None
        self._dic[_Fields.PKEY1] = None
        self._dic[_Fields.PKEY2] = None

    def where(self, *fields):
        """
        Continues a Join transformation.

        Defines the Tuple fields of the first join DataSet that should be used as join keys.
        Note: Fields can only be selected as join keys on Tuple DataSets.

        :param fields: The indexes of the Tuple fields of the first join DataSets that should be used as keys.
        :return:An incomplete Join transformation.

        """
        self._dic[_Fields.KEY1] = fields
        return JoinOperatorTo(self._ref, self._dic)


class JoinOperatorTo(object):
    def __init__(self, ref, dic):
        self._ref = ref
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
        return JoinOperatorUsing(self._ref, self._dic)


class JoinOperatorUsing(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def project_first(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PKEY1] = fields
        return JoinOperatorProjectionA(self._ref, self._dic)

    def project_second(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PKEY2] = fields
        return JoinOperatorProjectionB(self._ref, self._dic)

    def using(self, operator, types):
        """
        Finalizes a Join transformation.

        Applies a JoinFunction to each pair of joined elements. Each JoinFunction call returns exactly one element.

        :param operator:The JoinFunction that is called for each pair of joined elements.
        :param types:
        :return:An Set that represents the joined result DataSet.
        """
        new_set = OperatorSet(self._dic[_Fields.PARENT], self._ref._env)
        self._dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        self._dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._dic[_Fields.TYPES] = types
        self._dic[_Fields.CHILD] = new_set
        self._ref._env._sets.append(self._dic)
        return new_set


class JoinOperatorProjectionA(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def project_second(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PKEY2] = fields
        return JoinOperatorTypes(self._ref, self._dic)

    def types(self, *types):
        """
        Finalizes a ProjectJoin transformation.

        :param types: The type of the resulting DataSet.
        :return: The projected data set.
        """
        new_set = DataSet(self._ref._env)
        self._dic[_Fields.CHILD] = new_set
        self._dic[_Fields.TYPES] = types
        self._ref._env._sets.append(self._dic)
        return new_set


class JoinOperatorProjectionB(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def project_first(self, *fields):
        """
        Initiates a ProjectJoin transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete JoinProjection.
        """
        self._dic[_Fields.PKEY1] = fields
        return JoinOperatorTypes(self._ref, self._dic)

    def types(self, *types):
        """
        Finalizes a ProjectJoin transformation.

        :param types: The type of the resulting DataSet.
        :return: The projected data set.
        """
        new_set = DataSet(self._ref._env)
        self._dic[_Fields.CHILD] = new_set
        self._dic[_Fields.TYPES] = types
        self._ref._env._sets.append(self._dic)
        return new_set


class JoinOperatorTypes(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def types(self, *types):
        """
        Finalizes a ProjectJoin transformation.

        :param types: The type of the resulting DataSet.
        :return: The projected data set.
        """
        new_set = DataSet(self._ref._env)
        self._dic[_Fields.CHILD] = new_set
        self._dic[_Fields.TYPES] = types
        self._ref._env._sets.append(self._dic)
        return new_set


class CrossOperator(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic
        self._dic[_Fields.PKEY1] = []
        self._dic[_Fields.PKEY2] = []
        self._dic[_Fields.OPERATOR] = []
        self._dic[_Fields.META] = []


    def project_first(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PKEY1] = fields
        return CrossOperatorProjectionA(self._ref, self._dic)

    def project_second(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PKEY2] = fields
        return CrossOperatorProjectionB(self._ref, self._dic)

    def using(self, operator, types):
        """
        Finalizes a Cross transformation.

        Applies a CrossFunction to each pair of joined elements. Each CrossFunction call returns exactly one element.

        :param operator:The CrossFunction that is called for each pair of joined elements.
        :param types: THe type of the resulting DataSet.
        :return:An Set that represents the joined result DataSet.
        """
        new_set = OperatorSet(self._dic[_Fields.PARENT], self._ref._env)
        self._dic[_Fields.OPERATOR] = pickle.dumps(operator, protocol=0)
        self._dic[_Fields.META] = str(inspect.getmodule(operator)) + "|" + str(operator.__class__.__name__)
        self._dic[_Fields.TYPES] = types
        self._dic[_Fields.CHILD] = new_set
        self._ref._env._sets.append(self._dic)
        return new_set


class CrossOperatorProjectionA(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def project_second(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the second join input.
        If the second join input is a Tuple DataSet, fields can be selected by their index.
        If the second join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PKEY2] = fields
        return CrossOperatorTypes(self._ref, self._dic)

    def types(self, *types):
        """
        Finalizes a ProjectCross transformation.

        :param types: The type of the resulting DataSet.
        :return: The projected data set.
        """
        new_set = DataSet(self._ref._env)
        self._dic[_Fields.CHILD] = new_set
        self._dic[_Fields.TYPES] = types
        self._ref._env._sets.append(self._dic)
        return new_set


class CrossOperatorProjectionB(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def project_first(self, *fields):
        """
        Initiates a ProjectCross transformation.

        Projects the first join input.
        If the first join input is a Tuple DataSet, fields can be selected by their index.
        If the first join input is not a Tuple DataSet, no parameters should be passed.

        :param fields: The indexes of the selected fields.
        :return: An incomplete CrossProjection.
        """
        self._dic[_Fields.PKEY1] = fields
        return CrossOperatorTypes(self._ref, self._dic)

    def types(self, *types):
        """
        Finalizes a ProjectCross transformation.

        :param types: The type of the resulting DataSet.
        :return: The projected data set.
        """
        new_set = DataSet(self._ref._env)
        self._dic[_Fields.CHILD] = new_set
        self._dic[_Fields.TYPES] = types
        self._ref._env._sets.append(self._dic)
        return new_set


class CrossOperatorTypes(object):
    def __init__(self, ref, dic):
        self._ref = ref
        self._dic = dic

    def types(self, *types):
        """
        Finalizes a ProjectCross transformation.

        :param types: The type of the resulting DataSet.
        :return: The projected data set.
        """
        new_set = DataSet(self._ref._env)
        self._dic[_Fields.CHILD] = new_set
        self._dic[_Fields.TYPES] = types
        self._ref._env._sets.append(self._dic)
        return new_set