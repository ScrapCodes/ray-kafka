#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import ray
from kafka import KafkaConsumer
from kafka import KafkaProducer

import argparse
import time
import uuid

parser = argparse.ArgumentParser()
parser.add_argument("--num-tasks",
                    help="total number of tasks to start", default=3)
parser.add_argument("--input-topic",
                    help="kafka input topic", default="test")
parser.add_argument("--output-topic",
                    help="kafka output topic", default="output")
parser.add_argument("--server",
                    help="kafka bootstrap server", default="localhost:9092")

@ray.remote
class KafkaForwarder(object):
    def __init__(self, inputTopic, outputTopic, bootstrapServer, shared_uuid):
        self.consumer=KafkaConsumer(inputTopic, bootstrap_servers=bootstrapServer,
                                      group_id=str(shared_uuid))
        self.producer=KafkaProducer(bootstrap_servers=bootstrapServer)
        self.outputTopic=outputTopic

    def read(self):
        for msg in self.consumer:
            self.producer.send(self.outputTopic, str(msg.timestamp))


if __name__ == "__main__":

    args = parser.parse_args()
    ray.init()
    
    tasks=args.num_tasks
    s_uuid=str(uuid.uuid4())
    kafkaForwarders=[KafkaForwarder.remote(args.input_topic,
        args.output_topic, args.server, s_uuid)
                     for _ in range(tasks)]
    
    # Execute f in parallel.
    
    object_ids = [kafkaForwarders[i].read.remote() for i in range(tasks)]
    results = ray.get(object_ids)
