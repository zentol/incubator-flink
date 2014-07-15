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
import numpy

from sklearn.cluster import KMeans

from flink.plan.Environment import get_environment
from flink.plan.Constants import Types
from flink.functions.GroupReduceFunction import GroupReduceFunction


class KMeans(GroupReduceFunction):
    def group_reduce(self, iterator, collector):
        data = iterator.all()
        est = KMeans(n_clusters=3)
        est.fit(data)
        labels = est.labels_
        collector.collect(labels.astype(numpy.int).tolist())


if __name__ == "__main__":
    env = get_environment()

    data = env.from_elements([(5.1, 3.5, 1.4, 0.2), (4.9, 3.0, 1.4, 0.2), (4.7, 3.2, 1.3, 0.2),
                              (4.6, 3.1, 1.5, 0.2), (5.0, 3.6, 1.4, 0.2), (5.4, 3.9, 1.7, 0.4),
                              (4.6, 3.4, 1.4, 0.3), (5.0, 3.4, 1.5, 0.2), (4.4, 2.9, 1.4, 0.2),
                              (4.9, 3.1, 1.5, 0.1)])

    data.groupreduce(
        KMeans(),
        [Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT]) \
        .output()

    env.execute()