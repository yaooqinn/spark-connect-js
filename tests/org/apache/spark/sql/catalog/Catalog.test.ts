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

import { DataTypes } from "../../../../../../src/org/apache/spark/sql/types";
import { StorageLevel } from "../../../../../../src/org/apache/spark/storage/StorageLevel";
import { sharedSpark, withDatabase } from "../../../../../helpers"

test("current database", async () => {
  const catalog = await sharedSpark.then(spark => spark.catalog);
  await (
    catalog.currentDatabase().then((currentDatabase) => {
      expect(currentDatabase).toBe("default");
    }));
});

test("set current database", async () => {
  const spark = await sharedSpark;
  await (
    spark.catalog.setCurrentDatabase("default").then(() => {
      return spark.catalog.currentDatabase().then((currentDatabase) => {
        expect(currentDatabase).toBe("default");
      });
    })
  );

  await withDatabase(spark, "test_db", async () => {
    await spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    return spark.catalog.currentDatabase().then((currentDatabase) => {
      expect(currentDatabase).toBe("default");
      return spark.catalog.setCurrentDatabase("test_db").then(async () => {
        return spark.catalog.currentDatabase().then((currentDatabase) => {
          expect(currentDatabase).toBe("test_db");
        });
      });
    });
  });
});

test("list databases", async () => {
  const spark = await sharedSpark;
  await withDatabase(spark, "test_db", async () => {
    await spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    await spark.catalog.listDatabases().collect().then((databases) => {
      expect(databases[0][0]).toBe("default");
      expect(databases[1][0]).toBe("test_db");
    });
    await spark.catalog.listDatabases('test_db').collect().then((databases) => {
      expect(databases[0][0]).toBe("test_db");
    });
  });
});

test("get database", async () => {
  const spark = await sharedSpark;
  await withDatabase(spark, "test_db", async () => {
    await spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    await spark.catalog.getDatabase("test_db").then((database) => {
      expect(database.name).toBe("test_db");
      expect(database.description).toBe("");
      expect(database.locationUri).toContain("test_db.db");
      expect(database.catalog).toEqual('spark_catalog');
    });
  });
});

test("database exists", async () => {
  const spark = await sharedSpark;
  await withDatabase(spark, "test_db", async () => {
    await spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    await spark.catalog.databaseExists("test_db").then((exists) => {
      expect(exists).toBe(true);
    });
  });
});


test("table apis", async () => {
  const spark = await sharedSpark;
  await withDatabase(spark, 'test_db', async () => {
    await spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    const schema = DataTypes.createStructType([
      DataTypes.createStructField("id", DataTypes.IntegerType, true),
      DataTypes.createStructField("name", DataTypes.StringType, true)
    ]);
    const options = new Map();
    await spark.catalog.createTable('test_db.t0', 'parquet', schema, 'comment t', options).show()
    await spark.sql('INSERT INTO test_db.t0 VALUES (1, "a"), (2, "b")')
    await spark.catalog.createTable('test_db.t1', 'orc', schema, options).show()
    await spark.sql('INSERT INTO test_db.t1 VALUES (1, "a"), (2, "b")')
    const dbDir = await spark.catalog.getDatabase('test_db').then((database) => database.locationUri)
    options.set("path", dbDir + "/t1")
    await spark.catalog.createTable('test_db.t2', 'orc', 'comment t2', options).show()
    await spark.catalog.createTable('test_db.t3', dbDir + "/t0").show() // parquet
    await spark.catalog.createTable('test_db.t4', dbDir + "/t1", 'orc').show() // orc
    await spark.catalog.listTables("test_db").collect().then((tables) => {
      expect(tables[0][0]).toBe("t0");
      expect(tables[1][0]).toBe("t1");
      expect(tables[2][0]).toBe("t2");
      expect(tables[3][0]).toBe("t3");
      expect(tables[4][0]).toBe("t4");
    });
    await spark.catalog.tableExists("test_db", "t0").then((exists) => {
      expect(exists).toBe(true);
    });
    await spark.catalog.tableExists("test_db.t0").then((exists) => {
      expect(exists).toBe(true);
    });
    await spark.catalog.tableExists("test_db", "t5").then((exists) => {
      expect(exists).toBe(false);
    });

    await spark.catalog.getTable("test_db.t0").then((table) => {
      expect(table.name).toBe("t0");
      expect(table.database).toBe("test_db");
      expect(table.description).toBe("comment t");
      expect(table.tableType).toBe("MANAGED");
    });
    await spark.catalog.getTable("test_db", "t0").then((table) => {
      expect(table.name).toBe("t0");
      expect(table.database).toBe("test_db");
      expect(table.description).toBe("comment t");
      expect(table.tableType).toBe("MANAGED");
    });

    await spark.catalog.cacheTable("test_db.t0")
    await spark.catalog.cacheTable("test_db.t1", StorageLevel.DISK_ONLY)
    let isCached = await spark.catalog.isCached("test_db.t0")
    expect(isCached).toBe(true);
    isCached = await spark.catalog.isCached("test_db.t1")
    expect(isCached).toBe(true);
    await spark.catalog.uncacheTable("test_db.t0")
    await spark.catalog.uncacheTable("test_db.t1")
    isCached = await spark.catalog.isCached("test_db.t0")
    expect(isCached).toBe(false);
    isCached = await spark.catalog.isCached("test_db.t1")
    expect(isCached).toBe(false);
    await spark.catalog.refreshByPath(dbDir + "/t0")
    await spark.catalog.refreshTable("test_db.t0")

    await spark.catalog.listColumns("test_db.t0").collect().then((columns) => {
      expect(columns[0][0]).toBe("id");
      expect(columns[0][2]).toBe("int");
      expect(columns[1][0]).toBe("name");
      expect(columns[1][2]).toBe("string");
    });

    await spark.catalog.listColumns("test_db", "t0").collect().then((columns) => {
      expect(columns[0][0]).toBe("id");
      expect(columns[0][2]).toBe("int");
      expect(columns[1][0]).toBe("name");
      expect(columns[1][2]).toBe("string");
    });
  });
}, 30000);

test("create external table", async () => {
  const spark = await sharedSpark;
  await withDatabase(spark, 'test_db', async () => {
    await spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    const schema = DataTypes.createStructType([
      DataTypes.createStructField("id", DataTypes.IntegerType, true),
      DataTypes.createStructField("value", DataTypes.StringType, true)
    ]);
    
    // First create a regular table with some data
    const options1 = new Map();
    await spark.catalog.createTable('test_db.source_table', 'parquet', schema, options1).show()
    await spark.sql('INSERT INTO test_db.source_table VALUES (1, "a"), (2, "b")')
    
    // Get the database location
    const dbDir = await spark.catalog.getDatabase('test_db').then((database) => database.locationUri)
    const sourcePath = dbDir + "/source_table"
    
    // Test createExternalTable with path only
    await spark.catalog.createExternalTable('test_db.ext_t1', sourcePath).show()
    
    // Test createExternalTable with path and source
    await spark.catalog.createExternalTable('test_db.ext_t2', sourcePath, 'parquet').show()
    
    // Test createExternalTable with source and options
    const options2 = new Map();
    options2.set("path", sourcePath)
    await spark.catalog.createExternalTable('test_db.ext_t3', 'parquet', options2).show()
    
    // Test createExternalTable with source, schema and options
    const options3 = new Map();
    options3.set("path", sourcePath)
    await spark.catalog.createExternalTable('test_db.ext_t4', 'parquet', schema, options3).show()
    
    // Verify the external tables exist and have correct type
    await spark.catalog.getTable("test_db.ext_t1").then((table) => {
      expect(table.name).toBe("ext_t1");
      expect(table.database).toBe("test_db");
      expect(table.tableType).toBe("EXTERNAL");
    });
    
    await spark.catalog.getTable("test_db.ext_t2").then((table) => {
      expect(table.name).toBe("ext_t2");
      expect(table.database).toBe("test_db");
      expect(table.tableType).toBe("EXTERNAL");
    });
    
    // Verify data can be read from external tables
    await spark.table("test_db.ext_t1").collect().then((rows) => {
      expect(rows.length).toBe(2);
    });
  });
}, 30000);

test("functions api", async () => {
  const spark = await sharedSpark;
  await withDatabase(spark, 'test_db', async () => {
    await spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    await spark.sql('CREATE FUNCTION test_db.f AS "org.apache.spark.sql.catalyst.expressions.Not"')
    await spark.catalog.listFunctions().collect().then((rows) => {
      expect(rows[0][0]).toBe('!');
      expect(rows[0][1]).toBeNull;
      expect(rows[0][2]).toBeNull;
      expect(rows[0][3]).toBe('! expr - Logical not.');
      expect(rows[0][4]).toBe('org.apache.spark.sql.catalyst.expressions.Not');
      expect(rows[0][5]).toBe(true);
    });
    await spark.catalog.listFunctions("test_db").collect().then((functions) => {
      expect(functions[0][0]).toBe("!");
    });
    await spark.catalog.listFunctions("test_db", "f").collect().then((functions) => {
      expect(functions.length).toBe(1);
      expect(functions[0][0]).toBe("f");
    });
    await spark.catalog.listFunctions("test_db", "f*").collect().then((functions) => {
      expect(functions.length).toBeGreaterThan(1);
    });
    await spark.catalog.functionExists("test_db", "f").then((exists) => {
      expect(exists).toBe(true);
    });
    await spark.catalog.functionExists("test_db", "kentyao").then((exists) => {
      expect(exists).toBe(false);
    });

    const f = await spark.catalog.getFunction("f")
    expect(f.name).toBe("f");
    expect(f.description).toBe("N/A.");
    expect(f.className).toBe("org.apache.spark.sql.catalyst.expressions.Not");
    const f1 = await spark.catalog.getFunction("test_db", "f")
    expect(f1.name).toBe("f");
  });
}, 30000);

test("catalog apis", async () => {
  const spark = await sharedSpark;
  await (
    spark.catalog.listCatalogs().collect().then((catalogs) => {
      expect(catalogs[0][0]).toBe("spark_catalog");
    })
  );
  await (
    spark.catalog.listCatalogs("spark*").collect().then((catalogs) => {
      expect(catalogs[0][0]).toBe("spark_catalog");
    })
  );

  await (
    spark.catalog.currentCatalog().then((functions) => {
      expect(functions).toBe("spark_catalog");
    })
  );

  try {
    await spark.catalog.setCurrentCatalog("invalid")
  } catch (e) {
    expect((e as Error).message).toContain("CATALOG_NOT_FOUND")
  }
});
