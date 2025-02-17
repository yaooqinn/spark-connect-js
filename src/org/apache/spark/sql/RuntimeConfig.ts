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
import * as b from "../../../../gen/spark/connect/base_pb";
import { Client } from "./grpc/Client";

export class RuntimeConfig {
  private client_: Client;
  constructor(client: Client) {
    this.client_ = client;
  }
    
  async set(k: string, v: string): Promise<b.ConfigResponse> {
    const setConf = create(b.ConfigRequest_SetSchema, {
      pairs: [create(b.KeyValueSchema, {key: k, value: v})]
    });
    return this.execute(op => {
      op.opType = { value: setConf, case: "set" }
    });
  }

  async setAll(m: Map<string, string>): Promise<b.ConfigResponse> {
    const pairs = Array.from(m.entries()).map(([k, v]) => {
      return create(b.KeyValueSchema, {key: k, value: v});
    });
    const setAllConf = create(b.ConfigRequest_SetSchema, {
      pairs: pairs
    });
    return this.execute(op => {
      op.opType = { value: setAllConf, case: "set" }
    });
  }

  async unset(k: string): Promise<b.ConfigResponse> {
    const unsetConf = create(b.ConfigRequest_UnsetSchema, {
      keys: [k]
    });
    return this.execute(op => {
      op.opType = { value: unsetConf, case: "unset" }
    });
  }
    
  async getOption(k: string): Promise<string | undefined> {
    return this.get(k).then(v => {
      return v;
    }).catch(() => { 
      return undefined;
    });
  }

  async get(k: string, defaultVal?: string): Promise<string> {
    let resp: Promise<b.ConfigResponse>;
    if (defaultVal) {
      const getConf = create(b.ConfigRequest_GetWithDefaultSchema, {
        pairs: [create(b.KeyValueSchema, {key: k, value: defaultVal})]
      })
      resp = this.execute(op => {
        op.opType = { value: getConf, case: "getWithDefault" }
      });
    } else {
      const getConf = create(b.ConfigRequest_GetSchema, { keys: [k] });
      resp = this.execute(op => {
        op.opType = { value: getConf, case: "get" }
      });
    }
    return resp.then(r => {
        // FIXME: assert pair length === 1
        if (r.pairs[0].value) {
          return r.pairs[0].value;
        } else {
          throw new Error(`Config: ${k} not found`);
        }
    });
  }

  async getAll(): Promise<Map<string, string | undefined>> {
    const getAll = create(b.ConfigRequest_GetAllSchema, {});
    return this.execute(op => {
      op.opType = { value: getAll, case: "getAll" }
    }).then(r => {
      const m = new Map<string, string | undefined>();
      r.pairs.forEach(p => {
        m.set(p.key, p.value);
      });
      return m;
    });
  }

  async isModifiable(k: string): Promise<boolean> {
    const isModifiable = create(b.ConfigRequest_IsModifiableSchema, {
      keys: [k]
    });
    const resp = this.execute(op => {
      op.opType = { value: isModifiable, case: "isModifiable" }
    });
    const r = await resp;
    // FIXME: assert pair length
    if (r.pairs[0].value) {
      return r.pairs[0].value.toLowerCase() === "true";
    }
    return false;
  }

  private execute(func: (op: b.ConfigRequest_Operation) => void): Promise<b.ConfigResponse> {
    const cro = create(b.ConfigRequest_OperationSchema, {})
    func(cro);
    const resp = this.client_.config(cro);
    return resp;
  }
}

