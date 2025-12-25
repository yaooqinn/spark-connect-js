/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { create } from "@bufbuild/protobuf";
import { Client } from "../../../../../src/org/apache/spark/sql/grpc/Client";
import * as connect from "../../../../../src/gen/spark/connect/base_pb";
import { AnalyzePlanResponseHandler } from "../../../../../src/org/apache/spark/sql/proto/AnalyzePlanResponeHandler";

const integrationUrl = process.env.SPARK_CONNECT_TEST_URL ?? process.env.SPARK_CONNECT_URL;
const integrationEnabled = !!integrationUrl;
const connectionString = integrationUrl ?? "sc://localhost:15002;user_id=yao;user_name=kent";
const integrationTest = integrationEnabled ? test : test.skip;

async function withClient<T>(f: (client: Client) => Promise<T>): Promise<T> {
  const builder = Client.builder();
  builder.connectionString(connectionString);
  const client = builder.build();
  return f(client);
}

test('Client Basic', async () => {
  const builder = Client.builder();
  builder.connectionString("sc://localhost:15002;user_id=yao;user_name=kent;session_id=6ec0bd7f-11c0-43da-975e-2a8ad9ebae0b;a=b;c=d");
  const client = builder.build();
  const conf = client.conf_;
  expect(conf).toBeDefined();
  const uc = conf.get_user_context();
  expect(uc.userId).toBe("yao");
  expect(uc.userName).toBe("kent");
  const metadata = conf.get_metadata().getMap();
  const expectedMeta = {"a": "b", "c": "d"};
  expect(metadata).toStrictEqual(expectedMeta);
  expect(client.session_id_).toBe("6ec0bd7f-11c0-43da-975e-2a8ad9ebae0b");
});

integrationTest("get all configs", async () => {
  withClient(async client => {
    const getAll = create(connect.ConfigRequest_GetAllSchema, {});
    const op = create(connect.ConfigRequest_OperationSchema,
      {
        opType: {
            value: getAll,
            case: "getAll"
        }
      });
    client.config(op).then(resp => {
      const configs = new Map<string, string | undefined>();
      resp.pairs.forEach(pair => {
        configs.set(pair.key, pair.value);
      });
      expect(configs.get("spark.executor.id")).toBe("driver");
      expect(configs.get("spark.master")).toBe("local[*]");
    });
    getAll.prefix = "spark.master";
    op.opType.value = getAll;
    return client.config(op).then(resp => {
      expect(resp.pairs.length).toBe(1);
      expect(resp.pairs[0].key).toBe("");
      expect(resp.pairs[0].value).toBe("local[*]");
    });
  });
});

integrationTest("analyze plan - sparkVersion", async () => {
  const resp = await withClient<AnalyzePlanResponseHandler>(async client => {
    return client.analyze(b => b.withSparkVersion()).then(resp => new AnalyzePlanResponseHandler(resp));
  });
  expect(resp.version).toContain("4.");
});
