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
import socket
import struct
#argv[1] = port to receive operator
#argv[2] = import string
#argv[3] = input file
#argv[4] = output file
#argv[5] = port


try:
    import dill
    s1 = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    s1.bind((socket.gethostname(), 0))
    s1.sendto(struct.pack(">i", s1.getsockname()[1]), (socket.gethostname(), int(sys.argv[1])))
    opSize = struct.unpack(">i", s1.recv(4))[0]
    serialized_operator = s1.recv(opSize)
    exec(sys.argv[2])
    operator = dill.loads(serialized_operator)
    operator._configure(sys.argv[3], sys.argv[4], int(sys.argv[5]))
    operator._go()
    sys.stdout.flush()
    sys.stderr.flush()
except:
    sys.stdout.flush()
    sys.stderr.flush()
    s = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    s.bind((socket.gethostname(), 0))
    destination = (socket.gethostname(), int(sys.argv[5]))
    s.sendto(struct.pack(">i", -2), destination)
    raise