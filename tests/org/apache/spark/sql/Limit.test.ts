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

import { sharedSpark } from "../../../../helpers";

test("limit api", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.limit(5).collect().then(rows => {
      expect(rows.length).toBe(5);
      expect(rows[0].getLong(0)).toBe(0n);
      expect(rows[1].getLong(0)).toBe(1n);
      expect(rows[2].getLong(0)).toBe(2n);
      expect(rows[3].getLong(0)).toBe(3n);
      expect(rows[4].getLong(0)).toBe(4n);
    });
  }));
});

test("head api", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.head(5).then(rows => {
      expect(rows.length).toBe(5);
      expect(rows[0].getLong(0)).toBe(0n);
      expect(rows[1].getLong(0)).toBe(1n);
      expect(rows[2].getLong(0)).toBe(2n);
      expect(rows[3].getLong(0)).toBe(3n);
      expect(rows[4].getLong(0)).toBe(4n);
    });
  }));
});

test("head api exceeds", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(3)").then(async df => {
    return df.head(5).then(rows => {
      expect(rows.length).toBe(3);
      expect(rows[0].getLong(0)).toBe(0n);
      expect(rows[1].getLong(0)).toBe(1n);
      expect(rows[2].getLong(0)).toBe(2n);
    });
  }));
});

test("head api default", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.head().then(row => {
      expect(row.getLong(0)).toBe(0n);
    });
  }));
});

test("first api", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.first().then(row => {
      expect(row.getLong(0)).toBe(0n);
    });
  }));
});

test("take api", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.take(5).then(rows => {
      expect(rows.length).toBe(5);
      expect(rows[0].getLong(0)).toBe(0n);
      expect(rows[1].getLong(0)).toBe(1n);
      expect(rows[2].getLong(0)).toBe(2n);
      expect(rows[3].getLong(0)).toBe(3n);
      expect(rows[4].getLong(0)).toBe(4n);
    });
  }));
});

test("offset api", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.offset(5).collect().then(rows => {
      expect(rows.length).toBe(5);
      expect(rows[0].getLong(0)).toBe(5n);
      expect(rows[1].getLong(0)).toBe(6n);
      expect(rows[2].getLong(0)).toBe(7n);
      expect(rows[3].getLong(0)).toBe(8n);
      expect(rows[4].getLong(0)).toBe(9n);
    });
  }));
});

test("tail api", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.tail(5).then(rows => {
      expect(rows.length).toBe(5);
      expect(rows[0].getLong(0)).toBe(5n);
      expect(rows[1].getLong(0)).toBe(6n);
      expect(rows[2].getLong(0)).toBe(7n);
      expect(rows[3].getLong(0)).toBe(8n);
      expect(rows[4].getLong(0)).toBe(9n);
    });
  }));
});


test("isEmpty api", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(0)").then(async df => {
    return df.isEmpty().then(isEmpty => {
      expect(isEmpty).toBe(true);
    });
  }));
});

test("isEmpty api false", async () => {
  const spark = await sharedSpark;
  return (spark.sql("SELECT * from range(10)").then(async df => {
    return df.isEmpty().then(isEmpty => {
      expect(isEmpty).toBe(false);
    });
  }));
});
