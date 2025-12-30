# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM apache/spark:4.1.0-scala2.13-java21-r-ubuntu

USER root

# Copy UDF source and compile it
COPY ./udf/src/main/scala/org/apache/spark/sql/test/StringConcat.scala /tmp/udf/
WORKDIR /tmp/udf
RUN scalac -classpath "/opt/spark/jars/*" StringConcat.scala && \
    jar cf spark-js-test-udfs.jar org/apache/spark/sql/test/*.class && \
    mv spark-js-test-udfs.jar /opt/spark/jars/ && \
    rm -rf /tmp/udf
WORKDIR /opt/spark

COPY ./entrypoint.sh /opt/spark-js/entrypoint.sh

EXPOSE 4040 15002
ENV HOSTNAME localhost
ENTRYPOINT [ "/opt/spark-js/entrypoint.sh" ]

# CMD ["/opt/spark/sbin/start-connect-server.sh && tail -f /opt/spark/logs/spark-root-org.apache.spark.sql.connect.service.SparkConnectServer-1-localhost.out"]
 
