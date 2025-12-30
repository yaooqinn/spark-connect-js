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

# Install sbt
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y sbt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy UDF project and build it with sbt
COPY ./udf /tmp/udf
WORKDIR /tmp/udf
RUN sbt package && \
    cp target/scala-2.13/spark-js-test-udfs_2.13-1.0.jar /opt/spark/jars/spark-js-test-udfs.jar && \
    rm -rf /tmp/udf
WORKDIR /opt/spark

COPY ./entrypoint.sh /opt/spark-js/entrypoint.sh

EXPOSE 4040 15002
ENV HOSTNAME localhost
ENTRYPOINT [ "/opt/spark-js/entrypoint.sh" ]

# CMD ["/opt/spark/sbin/start-connect-server.sh && tail -f /opt/spark/logs/spark-root-org.apache.spark.sql.connect.service.SparkConnectServer-1-localhost.out"]
 
