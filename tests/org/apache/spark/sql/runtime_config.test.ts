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

import { Client } from "../../../../../src/org/apache/spark/sql/grpc/Client";
import { RuntimeConfig } from "../../../../../src/org/apache/spark/sql/RuntimeConfig";

async function withClient(f: (client: Client) => Promise<void>) {
  const builder = Client.builder();
  builder.connectionString("sc://localhost:15002;user_id=yao;user_name=kent");
  const client = builder.build();
  await f(client);
}

test('runtime config - set, unset, get', async () => {
  await withClient(async client => {
    const config = new RuntimeConfig(client);
    await config.get("spark.executor.id").then(v => {
      expect(v).toBe("driver");
    });

    await config.get("spark.master").then(v => {
      expect(v).toBe("local[*]");
    });

    await config.set("spark.kent", "yao").then(async () => {
      await config.get("spark.kent").then(v => {
        expect(v).toBe("yao");
      });
    });
    await config.unset("spark.kent.not.exist").then(async () => {
      await config.getOption("spark.kent.not.exist").then(v => {
        expect(v).toBeUndefined();
      });
      await config.get("spark.kent.not.exist", "yao").then(async v => {
        expect(v).toBe("yao");
      });
    });
    await config.unset("spark.kent")
    await config.getOption("spark.kent").then(v => {
      expect(v).toBeUndefined();
    });
    await config.get("spark.kent").catch(e => {
      expect(e).toBeDefined();
      expect(e.message).toMatch("SQL_CONF_NOT_FOUND");
    });
  });
});

test("runtime config - get all configs", async () => {
  await withClient(async client => {
    const config = new RuntimeConfig(client);
    await config.getAll().then(configs => {
      expect(configs.get("spark.executor.id")).toBe("driver");
      expect(configs.get("spark.master")).toBe("local[*]");
    });
  });
});

test("runtime config - is modifiable", async () => {
  await withClient(async client => {
    const config = new RuntimeConfig(client);
    await config.isModifiable("spark.sql.warehouse.dir").then(v => {
      expect(v).toBe(false);
    });
    await config.isModifiable("spark.executor.id").then(v => {
      expect(v).toBe(false);
    });
    await config.isModifiable("spark.master").then(v => {
      expect(v).toBe(false);
    });
    await config.isModifiable("spark.sql.ansi.enabled").then(v => {
      expect(v).toBe(true);
    });
  });
});
