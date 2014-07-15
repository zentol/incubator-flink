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
import re

from flink.plan.Environment import get_environment
from flink.plan.Constants import Types
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import WriteMode


class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        if len(value) > 0:
            words = re.split("\W+", value.lower())
            for word in words:
                collector.collect((1, word))


class Tokenizer2(FlatMapFunction):
    def flat_map(self, value, collector):
        if len(value) > 0:
            words = re.split("\W+", value.lower())
            for word in words:
                if len(word) < 50:
                    collector.collect((1, word))


class Tokenizer3(FlatMapFunction):
    def flat_map(self, value, collector):
        if len(value) > 0:
            words = re.split("\W+", value.lower())
            for word in words:
                if len(word) > 0:
                    collector.collect((1, word))


class Tokenizer4(FlatMapFunction):
    def flat_map(self, value, collector):
        if len(value) > 0:
            words = re.split("\W+", value.lower())
            for word in words:
                if len(word) < 50 and len(word) > 0:
                    collector.collect((1, word))


class Adder(GroupReduceFunction):
    def group_reduce(self, iterator, collector):
        first = iterator.next()
        count = first[0]
        word = first[1]

        while iterator.has_next():
            count += iterator.next()[0]

        collector.collect((count, word))


if __name__ == "__main__":
    env = get_environment()
    data = env.read_text("hdfs:/datasets/enwiki-latest-pages-meta-current.xml")

    data \
        .flatmap(Tokenizer3(), (Types.INT, Types.STRING)) \
        .group_by(1) \
        .groupreduce(Adder(), (Types.INT, Types.STRING)) \
        .write_csv("hdfs:/tmp/python/output/", write_mode=WriteMode.OVERWRITE)

    env.set_degree_of_parallelism(200)

    env.execute()