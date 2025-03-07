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

import { ClientBuilder } from "../../../../../src/org/apache/spark/sql/grpc/client_builder";

test("connection string to builder", () => {
    const builder = new ClientBuilder();
    builder.connectionString("sc://localhost:15002;user_id=yao;user_name=kent;session_id=6ec0bd7f-11c0-43da-975e-2a8ad9ebae0b;a=b;c=d");
    const conf = builder.conf();
    expect(conf).toBeDefined();
    const uc = conf.get_user_context();
    expect(uc.userId).toBe("yao");
    expect(uc.userName).toBe("kent");
    const metadata = conf.get_metadata().getMap();
    const expectedMeta = {"a": "b", "c": "d"}
    expect(metadata).toStrictEqual(expectedMeta);
});


test("invalid session id", () => {
    const builder = new ClientBuilder();
    expect(() => builder.connectionString("sc://localhost:15002;session_id=invalid")).toThrowError("Invalid session_id: invalid");
});
