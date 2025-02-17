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

import { SparkSession } from "../../../../../../src/org/apache/spark/sql/SparkSession"
import { Float32, tableFromIPC, Type, vectorFromArray } from 'apache-arrow';
import { SparkResult } from "../../../../../../src/org/apache/spark/sql/SparkResult";
import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types";
import { Database } from "../../../../../../src/org/apache/spark/sql/catalog/Database";

const spark = await SparkSession.builder()
  .appName("example")
  .getOrCreate();

// spark.sql("CREATE TABLE IF NOT EXISTS spark_connect_typecript(" +
//   "id int, name string, age int, salary double, start date)" +
//   "using parquet").then(df => {
//   console.log(df.schema());
//   df.explain("extended")
// }).then(() => {
//   spark.sql("INSERT INTO spark_connect_typecript VALUES (1, 'Alice', 30, 1000.0, date'2021-01-01')").then(df => {
//     df.explain("formatted")
//   })
//   spark.sql("INSERT INTO spark_connect_typecript VALUES (2, 'Bob', 40, 2000.0, date'2021-01-02')").then(df => {
//     df.explain("cost")
//   });
// });
// spark.sql("SELECT * FROM spark_connect_typecript where id < 3").then(df => {
//   df.explain(false)
//   console.log(df.schema());
// }).catch(err => {
//   console.log(err)
// });

// spark.read().json(__dirname + "/data/people.json", __dirname + "/data/people.json").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().csv(__dirname + "/data/people.csv", __dirname + "/data/people.json").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().parquet(__dirname + "/data/users.parquet").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().format("json").load(__dirname + "/data/people.json").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().format("csv").load(__dirname + "/data/people.csv").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().format("parquet").load(__dirname + "/data/users.parquet").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().orc(__dirname + "/data/users.orc").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().table("spark_connect_typecript").schema().then(schema => {
//   console.log(schema);
// });

// spark.read().parquet(__dirname + "/data/users.parquet")
//   .write()
//   .mode("overwrite")
//   .saveAsTable("spark_connect_typecript_2").then(resp => {
//     console.log(resp);
//     spark.sql("SELECT * FROM spark_connect_typecript_2").then(df => {
//       df.collect().then(resps => {
//         new SparkResult<boolean>(resps[Symbol.iterator]()).processResponses();
//         resps.forEach(resp => {
//           const respType = resp.responseType
//           if (respType.case === "arrowBatch") {
//             const table = tableFromIPC(respType.value.data);
//             console.table(table.toArray());
//           } else {
//             // console.log(respType);
//           }
//         });
//       });
//     });
//   });

  spark.sql(`SELECT
    1Y AS B,
    2S AS S,
    3 AS I,
    9007199254740991L AS L,
    5.6f AS F,
    7.8d AS D,
    9.0bd AS BD,
    'My way or high way' AS ST,
    X'10011' AS BIN,
    date'2018-11-17' AS DATE,
    timestamp'2018-11-17 13:33:33.333' AS TIMESTAMP,
    timestamp_ntz'2018-11-17 13:33:33.333' AS TIMESTAMP_NTZ,
    array(1, 2, 3) AS ARR,
    array('x', 'y', 'z') AS STR_ARR,
    array(1.0f, 2.0f, 3.0f) AS FARR,
    array(1.0bd, 2.0bd, 9999999999999.99bd) AS FARR2,
    array(array(1, 2, 3), array(4, 5, 6)) AS ARR2,
    map(1, 'a', 2, 'b') AS MAP1,
    map(1, 2, 3, 4) AS MAP2,
    map(1, array(1, 2, 3), 2, array(4, 5, 6)) AS MAP3,
    map(1, map(1, 'a', 2, 'b'), 2, map(3, 'c', 4, 'd')) AS MAP4,
    named_struct('a', array('x', 'y', 'z', null), 'b', map(1, 'a', 2, 'b'), 'c', named_struct('a', 1.0)) AS NAMED_STRUCT
    from range(1)`).then(async df => {
    // df.collect().then(row => console.log(row));
    await df.show(20, false, true);
    const res = await df.collect();
    // res.forEach(row => {
    //   console.log('=======\n', row);
    // });
    const schema = await df.schema();
    const df2 = await spark.createDataFrame(res, schema);
    await df2.show(20, false, true);
    const res2 = await df2.collect();
    // res2.forEach(row => {
    //   console.log('=======\n', row);
    //   console.log('=======\n', row.toJSON());
    // });
  });
  spark.sql("create database if not exists test").then(() => {
    spark.catalog.setCurrentDatabase("test").then(() => {
      spark.catalog.currentDatabase().then(db => {
        console.log(db);
        spark.sql("create table if not exists test.test_table (id int, name string)").then(() => {
          spark.catalog.getTable("test", "test_table").then(table => {
            console.log(table);
          });
        });
      });

    spark.catalog.listDatabases().collect().then(dbs => {
      dbs.forEach(db => {
        console.log(db);
      });
    });

    spark.catalog.listColumns("test").collect().then(cols => {
      cols.forEach(col => {
        console.log(col);
      });
    });

    spark.sql("drop database if exists test").then(() => {});
  });
});