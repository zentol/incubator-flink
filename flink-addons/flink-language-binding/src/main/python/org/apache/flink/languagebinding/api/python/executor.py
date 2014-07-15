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
import sys

#argv[1] = mode
#argv[2] = serialized operator / script path
#argv[3] = import string / -
if sys.argv[1] == "0":
    try:
        import cPickle as pickle
    except:
        import pickle

    exec (sys.argv[3])

    serialized_operator = sys.argv[2]
    operator = pickle.loads(serialized_operator)
    operator.run()
elif sys.argv[1] == "1":
    exec (sys.argv[2])