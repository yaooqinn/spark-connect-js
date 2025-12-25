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

import { bigintToDecimalBigNum, DecimalBigNumToNumber, getAsPlainJS } from '../../../../../src/org/apache/spark/sql/arrow/ArrowUtils';
import { DataTypes } from '../../../../../src/org/apache/spark/sql/types/DataTypes';
import { StructType } from '../../../../../src/org/apache/spark/sql/types/StructType';
import { sharedSpark } from '../../../../helpers';

test("null", async () => {
  const spark = await sharedSpark;
  await (spark.sql("SELECT null as a").then(df => {
    return df.collect().then(rows => {
      expect(rows[0].isNullAt(0)).toBe(true);
      return df.schema().then(schema => {
        expect(schema.fields[0].dataType).toBe(DataTypes.NullType);
        expect(schema.fields[0].name).toBe("a");
        expect(schema.fields[0].nullable).toBe(true);
        return spark.createDataFrame(rows, schema).collect().then(rows2 => {
          expect(rows2[0].isNullAt(0)).toBe(true);
          expect(rows2[0].get(0)).toBe(null);
          expect(rows2[0][0]).toBe(null);
        });
      });
    });
  }));
});

test("boolean", async () => {
  const spark = await sharedSpark;
  await (spark.sql("SELECT true as a").then(df => {
    return df.collect().then(rows => {
      expect(rows[0].getBoolean(0)).toBe(true);
      return df.schema().then(schema => {
        expect(schema.fields[0].dataType).toBe(DataTypes.BooleanType);
        expect(schema.fields[0].name).toBe("a");
        expect(schema.fields[0].nullable).toBe(false);
        return spark.createDataFrame(rows, schema).collect().then(rows2 => {
          expect(rows2[0].getBoolean(0)).toBe(true);
          expect(rows2[0].get(0)).toBe(true);
          expect(rows2[0].getAs<boolean>(0)).toBe(true);
          expect(rows2[0][0]).toBe(true);
        });
      });
    });
  }));
});

test("byte", async () => {
  const spark = await sharedSpark;
  await (spark.sql("SELECT cast(1 as byte) as a, 127Y `b.c`").then(df => {
    return df.collect().then(rows => {
      expect(rows[0].getByte(0)).toBe(1);
      expect(rows[0].getByte(1)).toBe(127);
      return df.schema().then(schema => {
        expect(schema.fields[0].dataType).toBe(DataTypes.ByteType);
        expect(schema.fields[0].name).toBe("a");
        expect(schema.fields[0].nullable).toBe(false);
        expect(schema.fields[1].dataType).toBe(DataTypes.ByteType);
        expect(schema.fields[1].name).toBe("b.c");
        expect(schema.fields[1].nullable).toBe(false);
        return spark.createDataFrame(rows, schema).collect().then(rows2 => {
          expect(rows2[0].getByte(0)).toBe(1);
          expect(rows2[0].get(0)).toBe(1);
          expect(rows2[0].getAs<number>(0)).toBe(1);
          expect(rows2[0][0]).toBe(1);
          expect(rows2[0].getByte(1)).toBe(127);
          expect(rows2[0].get(1)).toBe(127);
          expect(rows2[0].getAs<number>(1)).toBe(127);
          expect(rows2[0][1]).toBe(127);
        });
      });
    });
  }));
});


test("short", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT cast(1 as short) as a, 32767S `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getShort(0)).toBe(1);
        expect(rows[0].getShort(1)).toBe(32767);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.ShortType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.ShortType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getShort(0)).toBe(1);
            expect(rows2[0].get(0)).toBe(1);
            expect(rows2[0].getAs<number>(0)).toBe(1);
            expect(rows2[0][0]).toBe(1);
            expect(rows2[0].getShort(1)).toBe(32767);
            expect(rows2[0].get(1)).toBe(32767);
            expect(rows2[0].getAs<number>(1)).toBe(32767);
            expect(rows2[0][1]).toBe(32767);
          });
        });
      });
  }));
});

test("integer", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1 as a, 2147483647 `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getInt(0)).toBe(1);
        expect(rows[0].getInt(1)).toBe(2147483647);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.IntegerType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.IntegerType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getInt(0)).toBe(1);
            expect(rows2[0].get(0)).toBe(1);
            expect(rows2[0].getAs<number>(0)).toBe(1);
            expect(rows2[0][0]).toBe(1);
            expect(rows2[0].getInt(1)).toBe(2147483647);
            expect(rows2[0].get(1)).toBe(2147483647);
            expect(rows2[0].getAs<number>(1)).toBe(2147483647);
            expect(rows2[0][1]).toBe(2147483647);
          });
        });
      });
  }));
});

test("long", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1L as a, 9223372036854775807L `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getLong(0)).toBe(1n);
        expect(rows[0].getLong(1)).toBe(9223372036854775807n);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.LongType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.LongType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getLong(0)).toBe(1n);
            expect(rows2[0].get(0)).toBe(1n);
            expect(rows2[0].getAs<bigint>(0)).toBe(1n);
            expect(rows2[0][0]).toBe(1n);
            expect(rows2[0].getLong(1)).toBe(9223372036854775807n);
            expect(rows2[0].get(1)).toBe(9223372036854775807n);
            expect(rows2[0].getAs<bigint>(1)).toBe(9223372036854775807n);
            expect(rows2[0][1]).toBe(9223372036854775807n);
          });
        });
      });
  }));
});

test("float", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0f as a, 1.0e-8f `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getFloat(0)).toBeCloseTo(1.0);
        expect(rows[0].getFloat(1)).toBeCloseTo(1.0e-8, 10);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.FloatType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.FloatType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getFloat(0)).toBeCloseTo(1.0);
            expect(rows2[0].get(0)).toBeCloseTo(1.0);
            expect(rows2[0].getAs<number>(0)).toBeCloseTo(1.0);
            expect(rows2[0][0]).toBeCloseTo(1.0);
            expect(rows2[0].getFloat(1)).toBeCloseTo(1.0e-8, 10);
            expect(rows2[0].get(1)).toBeCloseTo(1.0e-8, 10);
            expect(rows2[0].getAs<number>(1)).toBeCloseTo(1.0e-8, 10);
            expect(rows2[0][1]).toBeCloseTo(1.0e-8, 10);
          });
        });
      });
  }));
});


test("double", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0d as a, 1.0e-8d `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getDouble(0)).toBeCloseTo(1.0);
        expect(rows[0].getDouble(1)).toBeCloseTo(1.0e-8, 10);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.DoubleType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.DoubleType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getDouble(0)).toBeCloseTo(1.0);
            expect(rows2[0].get(0)).toBeCloseTo(1.0);
            expect(rows2[0].getAs<number>(0)).toBeCloseTo(1.0);
            expect(rows2[0][0]).toBeCloseTo(1.0);
            expect(rows2[0].getDouble(1)).toBeCloseTo(1.0e-8, 10);
            expect(rows2[0].get(1)).toBeCloseTo(1.0e-8, 10);
            expect(rows2[0].getAs<number>(1)).toBeCloseTo(1.0e-8, 10);
            expect(rows2[0][1]).toBeCloseTo(1.0e-8, 10);
          });
        });
      });
  }));
});

test("decimal", async () => {
  const max = Number.MAX_SAFE_INTEGER
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 1.0bd as a, 1.0e-8bd `b.c`" + `, ${max}.123456 as d`).then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getDecimal(0)).toBe(1.0);
        expect(rows[0].getDecimal(1)).toBe(1.0e-8);
        expect(rows[0].getDecimal(2)).toBe(max + 0.123456);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(DataTypes.createDecimalType(2, 1));
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(DataTypes.createDecimalType(9, 9));
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getDecimal(0)).toBe(1.0);
            expect(rows2[0].get(0)).toBe(1.0);
            expect(rows2[0].getAs<number>(0)).toBe(1.0);
            expect(DecimalBigNumToNumber(rows2[0][0], 1)).toBe(1.0);
            expect(rows2[0].getDecimal(1)).toBe(1.0e-8);
            expect(rows2[0].get(1)).toBe(1.0e-8);
            expect(rows2[0].getAs<number>(1)).toBe(1.0e-8);
            expect(DecimalBigNumToNumber(rows2[0][1], 9)).toBe(1.0e-8);
            expect(rows2[0].getDecimal(2)).toBe(max + 0.123456);
          });
        });
      });
  }));
});

test("string", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT 'a' as a, 'b' `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getString(0)).toBe("a");
        expect(rows[0].getString(1)).toBe("b");
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.StringType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.StringType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getString(0)).toBe("a");
            expect(rows2[0].get(0)).toBe("a");
            expect(rows2[0].getAs<string>(0)).toBe("a");
            expect(rows2[0][0]).toBe("a");
            expect(rows2[0].getString(1)).toBe("b");
            expect(rows2[0].get(1)).toBe("b");
            expect(rows2[0].getAs<string>(1)).toBe("b");
            expect(rows2[0][1]).toBe("b");
          });
        });
      });
  }));
});

test("binary", async () => {
  const spark = await sharedSpark;
  const buffer1 = Buffer.from("apache");
  const buffer2 = Buffer.from("spark");
  const uint8Array1 = new Uint8Array(buffer1);
  const uint8Array2 = new Uint8Array(buffer2);
  await (
    spark.sql("SELECT cast('apache' as binary) as a, cast('spark' as binary) `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getBinary(0)).toStrictEqual(buffer1);
        expect(rows[0].getUint8Array(0)).toStrictEqual(uint8Array1);
        expect(rows[0].getBinary(1)).toStrictEqual(buffer2);
        expect(rows[0].getUint8Array(1)).toStrictEqual(uint8Array2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.BinaryType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.BinaryType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getBinary(0)).toStrictEqual(buffer1);
            expect(rows2[0].get(0)).toStrictEqual(uint8Array1);
            expect(rows2[0][0]).toStrictEqual(uint8Array1);
            expect(rows2[0].getBinary(1)).toStrictEqual(buffer2);
            expect(rows2[0].get(1)).toStrictEqual(uint8Array2);
            expect(rows2[0][1]).toStrictEqual(uint8Array2);
          });
        });
      });
  }));
});

test("date", async () => {
  const spark = await sharedSpark;
  await (
    spark.sql("SELECT date'2021-01-01' a, date'2018-11-17' `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0].getDate(0)).toStrictEqual(new Date("2021-01-01"));
        expect(rows[0].getDate(1)).toStrictEqual(new Date("2018-11-17"));
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.DateType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.DateType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0].getDate(0)).toStrictEqual(new Date("2021-01-01"));
            expect(rows2[0].get(0)).toStrictEqual(new Date("2021-01-01"));
            expect(rows2[0][0]).toBe(1609459200000);
            expect(rows2[0].getDate(1)).toStrictEqual(new Date("2018-11-17"));
            expect(rows2[0].get(1)).toStrictEqual(new Date("2018-11-17"));
            expect(rows2[0][1]).toBe(1542412800000);
          });
        });
      });
  }));
});

test("timestamp", async () => {
  const spark = await sharedSpark;
  const df = await spark.sql("SELECT timestamp'2021-01-01 01:00:00' a, timestamp'2018-11-17 13:33:33' `b.c`");
  const rows = await df.collect();
  const timezoneDf =  await spark.sql("SELECT timestamp'2021-01-01 01:00:00 Z' - timestamp'2021-01-01 01:00:00'")
  const timezone = await timezoneDf.head().then(row => row[0]);
  const timezoneNumber = Number(timezone / 1000n);
  const date1 = new Date("2021-01-01T01:00:00Z");
  const date2 = new Date("2018-11-17T13:33:33Z");
  const date3 = new Date(date1.getTime() - timezoneNumber);
  const date4 = new Date(date2.getTime() - timezoneNumber);
  expect(rows[0][0]).toBe(date3.getTime());
  expect(rows[0][1]).toBe(date4.getTime());
  expect(rows[0].getTimestamp(0)).toStrictEqual(date3);
  expect(rows[0].getTimestamp(1)).toStrictEqual(date4);
  const schema = await df.schema();
  expect(schema.fields[0].dataType).toBe(DataTypes.TimestampType);
  expect(schema.fields[0].name).toBe("a");
  expect(schema.fields[0].nullable).toBe(false);
  expect(schema.fields[1].dataType).toBe(DataTypes.TimestampType);
  expect(schema.fields[1].name).toBe("b.c");
  expect(schema.fields[1].nullable).toBe(false);
  const rows2 = await spark.createDataFrame(rows, schema).collect();
  expect(rows2[0].getTimestamp(0)).toStrictEqual(date3);
  expect(rows2[0].get(0)).toStrictEqual(date3);
  expect(rows2[0][0]).toBe(date3.getTime());
  expect(rows2[0].getTimestamp(1)).toStrictEqual(date4);
  expect(rows2[0].get(1)).toStrictEqual(date4);
  expect(rows2[0][1]).toBe(date4.getTime());
});

test("timestamp_ntz", async () => {
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT timestamp_ntz'2021-01-01 01:00:00' a, timestamp_ntz'2018-11-17 13:33:33' `b.c`").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toBe(1609462800000);
        expect(rows[0][1]).toBe(1542461613000);
        // expect(rows[0].getTimestamp(0)).toStrictEqual(new Date(`2021-01-01T01:00:00.000Z`));
        // expect(rows[0].getTimestamp(1)).toStrictEqual(new Date(`2018-11-17T13:33:33.000Z`));
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBe(DataTypes.TimestampNTZType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toBe(DataTypes.TimestampNTZType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            // expect(rows2[0].getTimestamp(0)).toStrictEqual(new Date("2021-01-01T01:00:00.000Z"));
            expect(rows2[0].get(0)).toStrictEqual(new Date("2021-01-01T01:00:00.000Z"));
            expect(rows2[0][0]).toBe(1609462800000);
            // expect(rows2[0].getTimestamp(1)).toStrictEqual(new Date("2018-11-17T13:33:33.000Z"));
            expect(rows2[0].get(1)).toStrictEqual(new Date(`2018-11-17T13:33:33.000Z`));
            expect(rows2[0][1]).toBe(1542461613000);
          });
        });
      });
  }));
});

test("boolean array", async () => {
  const booleanArray = [true, false, true];
  const booleanArray2 = [false, true, false];
  const booleanArrayType = DataTypes.createArrayType(DataTypes.BooleanType, false);
  const nestedBooleanArrayType = DataTypes.createArrayType(booleanArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(true, false, true) a, array(false, true, false) `b.c`, array(array(true, false, true), array(false, true, false)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(booleanArray);
        expect(rows[0].get(0)).toStrictEqual(booleanArray);
        expect(rows[0][1]).toStrictEqual(booleanArray2);
        expect(rows[0].get(1)).toStrictEqual(booleanArray2);
        expect(rows[0][2]).toStrictEqual([booleanArray, booleanArray2]);
        expect(rows[0].get(2)).toStrictEqual([booleanArray, booleanArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(booleanArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(booleanArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedBooleanArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(booleanArray);
            expect(rows2[0].get(0)).toStrictEqual(booleanArray);
            expect(rows2[0][1]).toStrictEqual(booleanArray2);
            expect(rows2[0].get(1)).toStrictEqual(booleanArray2);
            expect(rows2[0][2]).toStrictEqual([booleanArray, booleanArray2]);
            expect(rows2[0].get(2)).toStrictEqual([booleanArray, booleanArray2]);
          });
        });
      });
  }));
});

test("byte array", async () => {
  const byteArray = [1, 2, 3];
  const byteArray2 = [4, 5, 6];
  const byteArrayType = DataTypes.createArrayType(DataTypes.ByteType, false);
  const nestedByteArrayType = DataTypes.createArrayType(byteArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1Y, 2Y, 3Y) a, array(4Y, 5Y, 6Y) `b.c`, array(array(1Y, 2Y, 3Y), array(4Y, 5Y, 6Y)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(byteArray);
        expect(rows[0].get(0)).toStrictEqual(byteArray);
        expect(rows[0][1]).toStrictEqual(byteArray2);
        expect(rows[0].get(1)).toStrictEqual(byteArray2);
        expect(rows[0][2]).toStrictEqual([byteArray, byteArray2]);
        expect(rows[0].get(2)).toStrictEqual([byteArray, byteArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(byteArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(byteArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedByteArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(byteArray);
            expect(rows2[0].get(0)).toStrictEqual(byteArray);
            expect(rows2[0][1]).toStrictEqual(byteArray2);
            expect(rows2[0].get(1)).toStrictEqual(byteArray2);
            expect(rows2[0][2]).toStrictEqual([byteArray, byteArray2]);
            expect(rows2[0].get(2)).toStrictEqual([byteArray, byteArray2]);
          });
        });
      });
  }));
});

test("short array", async () => {
  const shortArray = [1, 2, 3];
  const shortArray2 = [4, 5, 6];
  const shortArrayType = DataTypes.createArrayType(DataTypes.ShortType, false);
  const nestedShortArrayType = DataTypes.createArrayType(shortArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1S, 2S, 3S) a, array(4S, 5S, 6S) `b.c`, array(array(1S, 2S, 3S), array(4S, 5S, 6S)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(shortArray);
        expect(rows[0].get(0)).toStrictEqual(shortArray);
        expect(rows[0][1]).toStrictEqual(shortArray2);
        expect(rows[0].get(1)).toStrictEqual(shortArray2);
        expect(rows[0][2]).toStrictEqual([shortArray, shortArray2]);
        expect(rows[0].get(2)).toStrictEqual([shortArray, shortArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(shortArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(shortArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedShortArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(shortArray);
            expect(rows2[0].get(0)).toStrictEqual(shortArray);
            expect(rows2[0][1]).toStrictEqual(shortArray2);
            expect(rows2[0].get(1)).toStrictEqual(shortArray2);
            expect(rows2[0][2]).toStrictEqual([shortArray, shortArray2]);
            expect(rows2[0].get(2)).toStrictEqual([shortArray, shortArray2]);
          });
        });
      });
  }));
});

test("integer array", async () => {
  const intArray = [1, 2, 3];
  const intArray2 = [4, 5, 6];
  const intArrayType = DataTypes.createArrayType(DataTypes.IntegerType, false);
  const nestedIntArrayType = DataTypes.createArrayType(intArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1, 2, 3) a, array(4, 5, 6) `b.c`, array(array(1, 2, 3), array(4, 5, 6)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(intArray);
        expect(rows[0].get(0)).toStrictEqual(intArray);
        expect(rows[0][1]).toStrictEqual(intArray2);
        expect(rows[0].get(1)).toStrictEqual(intArray2);
        expect(rows[0][2]).toStrictEqual([intArray, intArray2]);
        expect(rows[0].get(2)).toStrictEqual([intArray, intArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(intArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(intArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedIntArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(intArray);
            expect(rows2[0].get(0)).toStrictEqual(intArray);
            expect(rows2[0][1]).toStrictEqual(intArray2);
            expect(rows2[0].get(1)).toStrictEqual(intArray2);
            expect(rows2[0][2]).toStrictEqual([intArray, intArray2]);
            expect(rows2[0].get(2)).toStrictEqual([intArray, intArray2]);
          });
        });
      });
  }));
});

test("long array", async () => {
  const longArray = [1n, 2n, 3n];
  const longArray2 = [4n, 5n, 6n];
  const longArrayType = DataTypes.createArrayType(DataTypes.LongType, false);
  const nestedLongArrayType = DataTypes.createArrayType(longArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1L, 2L, 3L) a, array(4L, 5L, 6L) `b.c`, array(array(1L, 2L, 3L), array(4L, 5L, 6L)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(longArray);
        expect(rows[0].get(0)).toStrictEqual(longArray);
        expect(rows[0][1]).toStrictEqual(longArray2);
        expect(rows[0].get(1)).toStrictEqual(longArray2);
        expect(rows[0][2]).toStrictEqual([longArray, longArray2]);
        expect(rows[0].get(2)).toStrictEqual([longArray, longArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(longArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(longArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedLongArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(longArray);
            expect(rows2[0].get(0)).toStrictEqual(longArray);
            expect(rows2[0][1]).toStrictEqual(longArray2);
            expect(rows2[0].get(1)).toStrictEqual(longArray2);
            expect(rows2[0][2]).toStrictEqual([longArray, longArray2]);
            expect(rows2[0].get(2)).toStrictEqual([longArray, longArray2]);
          });
        });
      });
  }));
});

test("float array", async () => {
  const floatArray = [1.0, 2.0, 3.0];
  const floatArray2 = [4.0, 5.0, 6.0];
  const floatArrayType = DataTypes.createArrayType(DataTypes.FloatType, false);
  const nestedFloatArrayType = DataTypes.createArrayType(floatArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1.0f, 2.0f, 3.0f) a, array(4.0f, 5.0f, 6.0f) `b.c`, array(array(1.0f, 2.0f, 3.0f), array(4.0f, 5.0f, 6.0f)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(floatArray);
        expect(rows[0].get(0)).toStrictEqual(floatArray);
        expect(rows[0][1]).toStrictEqual(floatArray2);
        expect(rows[0].get(1)).toStrictEqual(floatArray2);
        expect(rows[0][2]).toStrictEqual([floatArray, floatArray2]);
        expect(rows[0].get(2)).toStrictEqual([floatArray, floatArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(floatArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(floatArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedFloatArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(floatArray);
            expect(rows2[0].get(0)).toStrictEqual(floatArray);
            expect(rows2[0][1]).toStrictEqual(floatArray2);
            expect(rows2[0].get(1)).toStrictEqual(floatArray2);
            expect(rows2[0][2]).toStrictEqual([floatArray, floatArray2]);
            expect(rows2[0].get(2)).toStrictEqual([floatArray, floatArray2]);
          });
        });
      });
  }));
});

test("double array", async () => {
  const doubleArray = [1.0, 2.0, 3.0];
  const doubleArray2 = [4.0, 5.0, 6.0];
  const doubleArrayType = DataTypes.createArrayType(DataTypes.DoubleType, false);
  const nestedDoubleArrayType = DataTypes.createArrayType(doubleArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1.0d, 2.0d, 3.0d) a, array(4.0d, 5.0d, 6.0d) `b.c`, array(array(1.0d, 2.0d, 3.0d), array(4.0d, 5.0d, 6.0d)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(doubleArray);
        expect(rows[0].get(0)).toStrictEqual(doubleArray);
        expect(rows[0][1]).toStrictEqual(doubleArray2);
        expect(rows[0].get(1)).toStrictEqual(doubleArray2);
        expect(rows[0][2]).toStrictEqual([doubleArray, doubleArray2]);
        expect(rows[0].get(2)).toStrictEqual([doubleArray, doubleArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(doubleArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(doubleArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedDoubleArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(doubleArray);
            expect(rows2[0].get(0)).toStrictEqual(doubleArray);
            expect(rows2[0][1]).toStrictEqual(doubleArray2);
            expect(rows2[0].get(1)).toStrictEqual(doubleArray2);
            expect(rows2[0][2]).toStrictEqual([doubleArray, doubleArray2]);
            expect(rows2[0].get(2)).toStrictEqual([doubleArray, doubleArray2]);
          });
        });
      });
  }));
});


test("decimal array", async () => {
  const decimalArray = [1.0, 2.0, 3.0];
  const decimalArray2 = [4.0, 5.0, 6.0];
  const decimalArrayType = DataTypes.createArrayType(DataTypes.createDecimalType(2, 1), false);
  const nestedDecimalArrayType = DataTypes.createArrayType(decimalArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1.0bd, 2.0bd, 3.0bd) a, array(4.0bd, 5.0bd, 6.0bd) `b.c`, array(array(1.0bd, 2.0bd, 3.0bd), array(4.0bd, 5.0bd, 6.0bd)) d").then(df => {
      return df.collect().then(rows => {
        expect(getAsPlainJS(decimalArrayType, rows[0][0])).toStrictEqual(decimalArray);
        expect(rows[0].get(0)).toStrictEqual(decimalArray);
        expect(getAsPlainJS(decimalArrayType, rows[0][1])).toStrictEqual(decimalArray2);
        expect(rows[0].get(1)).toStrictEqual(decimalArray2);
        expect(getAsPlainJS(nestedDecimalArrayType, rows[0][2])).toStrictEqual([decimalArray, decimalArray2]);
        expect(rows[0].get(2)).toStrictEqual([decimalArray, decimalArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(decimalArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(decimalArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedDecimalArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(getAsPlainJS(decimalArrayType, rows2[0][0])).toStrictEqual(decimalArray);
            expect(rows2[0].get(0)).toStrictEqual(decimalArray);
            expect(getAsPlainJS(decimalArrayType, rows2[0][1])).toStrictEqual(decimalArray2);
            expect(rows2[0].get(1)).toStrictEqual(decimalArray2);
            expect(getAsPlainJS(nestedDecimalArrayType, rows2[0][2])).toStrictEqual([decimalArray, decimalArray2]);
            expect(rows2[0].get(2)).toStrictEqual([decimalArray, decimalArray2]);
          });
        });
      });
  }));
});

test("string array", async () => {
  const stringArray = ["a", "b", "c"];
  const stringArray2 = ["d", "e", "f"];
  const stringArrayType = DataTypes.createArrayType(DataTypes.StringType, false);
  const nestedStringArrayType = DataTypes.createArrayType(stringArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array('a', 'b', 'c') a, array('d', 'e', 'f') `b.c`, array(array('a', 'b', 'c'), array('d', 'e', 'f')) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(stringArray);
        expect(rows[0].get(0)).toStrictEqual(stringArray);
        expect(rows[0][1]).toStrictEqual(stringArray2);
        expect(rows[0].get(1)).toStrictEqual(stringArray2);
        expect(rows[0][2]).toStrictEqual([stringArray, stringArray2]);
        expect(rows[0].get(2)).toStrictEqual([stringArray, stringArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(stringArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(stringArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedStringArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(stringArray);
            expect(rows2[0].get(0)).toStrictEqual(stringArray);
            expect(rows2[0][1]).toStrictEqual(stringArray2);
            expect(rows2[0].get(1)).toStrictEqual(stringArray2);
            expect(rows2[0][2]).toStrictEqual([stringArray, stringArray2]);
            expect(rows2[0].get(2)).toStrictEqual([stringArray, stringArray2]);
          });
        });
      });
  }));
});

test("binary array", async () => {
  const buffer1 = Buffer.from("apache");
  const buffer2 = Buffer.from("spark");
  const uint8Array1 = new Uint8Array(buffer1);
  const uint8Array2 = new Uint8Array(buffer2);
  const binaryArrayType = DataTypes.createArrayType(DataTypes.BinaryType, false);
  const nestedBinaryArrayType = DataTypes.createArrayType(binaryArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(cast('apache' as binary), cast('spark' as binary)) a, array(cast('spark' as binary), cast('apache' as binary)) `b.c`, array(array(cast('apache' as binary), cast('spark' as binary)), array(cast('spark' as binary), cast('apache' as binary))) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0][0]).toStrictEqual(uint8Array1);
        expect(rows[0][0][1]).toStrictEqual(uint8Array2);
        expect(rows[0].get(0)[0]).toStrictEqual(uint8Array1);
        expect(rows[0].get(0)[1]).toStrictEqual(uint8Array2);
        expect(rows[0][1][0]).toStrictEqual(uint8Array2);
        expect(rows[0][1][1]).toStrictEqual(uint8Array1);
        expect(rows[0].get(1)[0]).toStrictEqual(uint8Array2);
        expect(rows[0].get(1)[1]).toStrictEqual(uint8Array1);
        expect(rows[0][2][0][0]).toStrictEqual(uint8Array1);
        expect(rows[0][2][0][1]).toStrictEqual(uint8Array2);
        expect(rows[0][2][1][0]).toStrictEqual(uint8Array2);
        expect(rows[0][2][1][1]).toStrictEqual(uint8Array1);
        expect(rows[0].get(2)[0][0]).toStrictEqual(uint8Array1);
        expect(rows[0].get(2)[0][1]).toStrictEqual(uint8Array2);
        expect(rows[0].get(2)[1][0]).toStrictEqual(uint8Array2);
        expect(rows[0].get(2)[1][1]).toStrictEqual(uint8Array1);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(binaryArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(binaryArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedBinaryArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0][0]).toStrictEqual(uint8Array1);
            expect(rows2[0][0][1]).toStrictEqual(uint8Array2);
            expect(rows2[0].get(0)[0]).toStrictEqual(uint8Array1);
            expect(rows2[0].get(0)[1]).toStrictEqual(uint8Array2);
            expect(rows2[0][1][0]).toStrictEqual(uint8Array2);
            expect(rows2[0][1][1]).toStrictEqual(uint8Array1);
            expect(rows2[0].get(1)[0]).toStrictEqual(uint8Array2);
            expect(rows2[0].get(1)[1]).toStrictEqual(uint8Array1);
            expect(rows2[0][2][0][0]).toStrictEqual(uint8Array1);
            expect(rows2[0][2][0][1]).toStrictEqual(uint8Array2);
            expect(rows2[0][2][1][0]).toStrictEqual(uint8Array2);
            expect(rows2[0][2][1][1]).toStrictEqual(uint8Array1);
            expect(rows2[0].get(2)[0][0]).toStrictEqual(uint8Array1);
            expect(rows2[0].get(2)[0][1]).toStrictEqual(uint8Array2);
            expect(rows2[0].get(2)[1][0]).toStrictEqual(uint8Array2);
            expect(rows2[0].get(2)[1][1]).toStrictEqual(uint8Array1);
          });
        });
      });
  }));
});

test("date array", async () => {
  const dateArray = [new Date("2021-01-01"), new Date("2018-11-17"), new Date("2020-12-31")];
  const dateArray2 = [new Date("2020-12-31"), new Date("2018-11-17"), new Date("2021-01-01")];
  const dateArrayType = DataTypes.createArrayType(DataTypes.DateType, false);
  const nestedDateArrayType = DataTypes.createArrayType(dateArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(date'2021-01-01', date'2018-11-17', date'2020-12-31') a, array(date'2020-12-31', date'2018-11-17', date'2021-01-01') `b.c`, array(array(date'2021-01-01', date'2018-11-17', date'2020-12-31'), array(date'2020-12-31', date'2018-11-17', date'2021-01-01')) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(dateArray.map(d => d.getTime()));
        expect(rows[0].get(0)).toStrictEqual(dateArray);
        expect(rows[0][1]).toStrictEqual(dateArray2.map(d => d.getTime()));
        expect(rows[0].get(1)).toStrictEqual(dateArray2);
        expect(rows[0][2]).toStrictEqual([dateArray.map(d => d.getTime()), dateArray2.map(d => d.getTime())]);
        expect(rows[0].get(2)).toStrictEqual([dateArray, dateArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(dateArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(dateArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedDateArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(dateArray.map(d => d.getTime()));
            expect(rows2[0].get(0)).toStrictEqual(dateArray);
            expect(rows2[0][1]).toStrictEqual(dateArray2.map(d => d.getTime()));
            expect(rows2[0].get(1)).toStrictEqual(dateArray2);
            expect(rows2[0][2]).toStrictEqual([dateArray.map(d => d.getTime()), dateArray2.map(d => d.getTime())]);
            expect(rows2[0].get(2)).toStrictEqual([dateArray, dateArray2]);
          });
        });
      });
  }));
});

test("timestamp array", async () => {
  const spark = await sharedSpark;
  const timezone =  await spark.sql("SELECT timestamp'2021-01-01 01:00:00 Z' - timestamp'2021-01-01 01:00:00'").then(df => df.head()).then(row => row.get(0));
  const timezoneNumber = Number(timezone / 1000n);
  const df = await spark.sql("SELECT array(timestamp'2021-01-01 01:00:00', timestamp'2018-11-17 13:33:33', timestamp'2020-12-31 23:59:59') a, array(timestamp'2020-12-31 23:59:59', timestamp'2018-11-17 13:33:33', timestamp'2021-01-01 01:00:00') `b.c`, array(array(timestamp'2021-01-01 01:00:00', timestamp'2018-11-17 13:33:33', timestamp'2020-12-31 23:59:59'), array(timestamp'2020-12-31 23:59:59', timestamp'2018-11-17 13:33:33', timestamp'2021-01-01 01:00:00')) d");
  const timestampArray = [new Date("2021-01-01 01:00:00Z"), new Date("2018-11-17 13:33:33Z"), new Date("2020-12-31 23:59:59Z")];
  const timestampArray2 = [new Date("2020-12-31 23:59:59Z"), new Date("2018-11-17 13:33:33Z"), new Date("2021-01-01 01:00:00Z")];
  const timestampArray3 = timestampArray.map(d => new Date(d.getTime() - timezoneNumber));
  const timestampArray4 = timestampArray2.map(d => new Date(d.getTime() - timezoneNumber));
  const row = await df.head();
  expect(row[0]).toStrictEqual(timestampArray3.map(d => d.getTime()));
  expect(row.get(0)).toStrictEqual(timestampArray3);
  expect(row[1]).toStrictEqual(timestampArray4.map(d => d.getTime()));
  expect(row.get(1)).toStrictEqual(timestampArray4);
  expect(row[2]).toStrictEqual([timestampArray3.map(d => d.getTime()), timestampArray4.map(d => d.getTime())]);
  expect(row.get(2)).toStrictEqual([timestampArray3, timestampArray4]);
  const schema = await df.schema();
  const df2 = spark.createDataFrame([row], schema);
  const row2 = await df2.head();
  expect(row2[0]).toStrictEqual(timestampArray3.map(d => d.getTime()));
  expect(row2.get(0)).toStrictEqual(timestampArray3);
  expect(row2[1]).toStrictEqual(timestampArray4.map(d => d.getTime()));
  expect(row2.get(1)).toStrictEqual(timestampArray4);
  expect(row2[2]).toStrictEqual([timestampArray3.map(d => d.getTime()), timestampArray4.map(d => d.getTime())]);
  expect(row2.get(2)).toStrictEqual([timestampArray3, timestampArray4]);
});

test("map array", async () => {
  const mapArray = [new Map([["a", 1], ["b", 2], ["c", 3]]), new Map([["d", 4], ["e", 5], ["f", 6]])];
  const mapArray2 = [new Map([["d", 4], ["e", 5], ["f", 6]]), new Map([["a", 1], ["b", 2], ["c", 3]])];
  const mapArrayType = DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false), false);
  const nestedMapArrayType = DataTypes.createArrayType(mapArrayType, false);
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(map('a', 1, 'b', 2, 'c', 3), map('d', 4, 'e', 5, 'f', 6)) a, array(map('d', 4, 'e', 5, 'f', 6), map('a', 1, 'b', 2, 'c', 3)) `b.c`, array(array(map('a', 1, 'b', 2, 'c', 3), map('d', 4, 'e', 5, 'f', 6)), array(map('d', 4, 'e', 5, 'f', 6), map('a', 1, 'b', 2, 'c', 3))) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(mapArray);
        expect(rows[0].get(0)).toStrictEqual(mapArray);
        expect(rows[0][1]).toStrictEqual(mapArray2);
        expect(rows[0].get(1)).toStrictEqual(mapArray2);
        expect(rows[0][2]).toStrictEqual([mapArray, mapArray2]);
        expect(rows[0].get(2)).toStrictEqual([mapArray, mapArray2]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(mapArrayType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(mapArrayType);
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(nestedMapArrayType);
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(mapArray);
            expect(rows2[0].get(0)).toStrictEqual(mapArray);
            expect(rows2[0][1]).toStrictEqual(mapArray2);
            expect(rows2[0].get(1)).toStrictEqual(mapArray2);
            expect(rows2[0][2]).toStrictEqual([mapArray, mapArray2]);
            expect(rows2[0].get(2)).toStrictEqual([mapArray, mapArray2]);
          });
        });
      });
  }));
});


test("null array", async () => {
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(null, null) a, array(null, null) `b.c`, array(array(null, null), array(null, null)) d").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual([null, null]);
        expect(rows[0].get(0)).toStrictEqual([null, null]);
        expect(rows[0][1]).toStrictEqual([null, null]);
        expect(rows[0].get(1)).toStrictEqual([null, null]);
        expect(rows[0][2]).toStrictEqual([[null, null], [null, null]]);
        expect(rows[0].get(2)).toStrictEqual([[null, null], [null, null]]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.NullType, true));
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.NullType, true));
          expect(schema.fields[1].name).toBe("b.c");
          expect(schema.fields[1].nullable).toBe(false);
          expect(schema.fields[2].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.NullType, true), false));
          expect(schema.fields[2].name).toBe("d");
          expect(schema.fields[2].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual([null, null]);
            expect(rows2[0].get(0)).toStrictEqual([null, null]);
            expect(rows2[0][1]).toStrictEqual([null, null]);
            expect(rows2[0].get(1)).toStrictEqual([null, null]);
            expect(rows2[0][2]).toStrictEqual([[null, null], [null, null]]);
            expect(rows2[0].get(2)).toStrictEqual([[null, null], [null, null]]);
          });
        });
      });
  }));
});

test("empty array", async () => {
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array() a,  array(array(), array()) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual([]);
        expect(rows[0].get(0)).toStrictEqual([]);
        expect(rows[0][1]).toStrictEqual([[], []]);
        expect(rows[0].get(1)).toStrictEqual([[], []]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.NullType, false));
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.NullType, false), false));
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual([]);
            expect(rows2[0].get(0)).toStrictEqual([]);
            expect(rows2[0][1]).toStrictEqual([[], []]);
            expect(rows2[0].get(1)).toStrictEqual([[], []]);
          });
        });
      });
  }));
});

test("array with nulls", async () => {
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT array(1, null, 3) a, array(null, 5, null) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual([1, null, 3]);
        expect(rows[0].get(0)).toStrictEqual([1, null, 3]);
        expect(rows[0][1]).toStrictEqual([null, 5, null]);
        expect(rows[0].get(1)).toStrictEqual([null, 5, null]);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.IntegerType, true));
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.IntegerType, true));
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual([1, null, 3]);
            expect(rows2[0].get(0)).toStrictEqual([1, null, 3]);
            expect(rows2[0][1]).toStrictEqual([null, 5, null]);
            expect(rows2[0].get(1)).toStrictEqual([null, 5, null]);
          });
        });
      });
  }));
});

test("boolean/boolean map", async () => {
  const spark = await sharedSpark;
  const booleanMap = new Map([[true, false]]);
  const booleanMap2 = new Map([[false, true]]);
  const booleanMapType = DataTypes.createMapType(DataTypes.BooleanType, DataTypes.BooleanType, false);
  return (
    spark.sql("SELECT map(TRUE, FALSE) a, map(FALSE, TRUE) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(booleanMap);
        expect(rows[0].get(0)).toStrictEqual(booleanMap);
        expect(rows[0][1]).toStrictEqual(booleanMap2);
        expect(rows[0].get(1)).toStrictEqual(booleanMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(booleanMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(booleanMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(booleanMap);
            expect(rows2[0].get(0)).toStrictEqual(booleanMap);
            expect(rows2[0][1]).toStrictEqual(booleanMap2);
            expect(rows2[0].get(1)).toStrictEqual(booleanMap2);
          });
        });
      });
  }));
});

test("byte/byte map", async () => {
  const spark = await sharedSpark;
  const byteMap = new Map([[1, 2], [3, 4]]);
  const byteMap2 = new Map([[5, 6], [7, 8]]);
  const byteMapType = DataTypes.createMapType(DataTypes.ByteType, DataTypes.ByteType, false);
  return (
    spark.sql("SELECT map(1Y, 2Y, 3Y, 4Y) a, map(5Y, 6Y, 7Y, 8Y) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(byteMap);
        expect(rows[0].get(0)).toStrictEqual(byteMap);
        expect(rows[0][1]).toStrictEqual(byteMap2);
        expect(rows[0].get(1)).toStrictEqual(byteMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(byteMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(byteMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(byteMap);
            expect(rows2[0].get(0)).toStrictEqual(byteMap);
            expect(rows2[0][1]).toStrictEqual(byteMap2);
            expect(rows2[0].get(1)).toStrictEqual(byteMap2);
          });
        });
      });
  }));
});

test("short/short map", async () => {
  const spark = await sharedSpark;
  const shortMap = new Map([[1, 2], [3, 4]]);
  const shortMap2 = new Map([[5, 6], [7, 8]]);
  const shortMapType = DataTypes.createMapType(DataTypes.ShortType, DataTypes.ShortType, false);
  return (
    spark.sql("SELECT map(1S, 2S, 3S, 4S) a, map(5S, 6S, 7S, 8S) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(shortMap);
        expect(rows[0].get(0)).toStrictEqual(shortMap);
        expect(rows[0][1]).toStrictEqual(shortMap2);
        expect(rows[0].get(1)).toStrictEqual(shortMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(shortMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(shortMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(shortMap);
            expect(rows2[0].get(0)).toStrictEqual(shortMap);
            expect(rows2[0][1]).toStrictEqual(shortMap2);
            expect(rows2[0].get(1)).toStrictEqual(shortMap2);
          });
        });
      });
  }));
});

test("int/int map", async () => {
  const spark = await sharedSpark;
  const intMap = new Map([[1, 2], [3, 4]]);
  const intMap2 = new Map([[5, 6], [7, 8]]);
  const intMapType = DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType, false);
  return (
    spark.sql("SELECT map(1, 2, 3, 4) a, map(5, 6, 7, 8) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(intMap);
        expect(rows[0].get(0)).toStrictEqual(intMap);
        expect(rows[0][1]).toStrictEqual(intMap2);
        expect(rows[0].get(1)).toStrictEqual(intMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(intMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(intMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(intMap);
            expect(rows2[0].get(0)).toStrictEqual(intMap);
            expect(rows2[0][1]).toStrictEqual(intMap2);
            expect(rows2[0].get(1)).toStrictEqual(intMap2);
          });
        });
      });
  }));
});

test("long/long map", async () => {
  const spark = await sharedSpark;
  const longMap = new Map([[1n, 2n], [3n, 4n]]);
  const longMap2 = new Map([[5n, 6n], [7n, 8n]]);
  const longMapType = DataTypes.createMapType(DataTypes.LongType, DataTypes.LongType, false);
  return (
    spark.sql("SELECT map(1L, 2L, 3L, 4L) a, map(5L, 6L, 7L, 8L) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(longMap);
        expect(rows[0].get(0)).toStrictEqual(longMap);
        expect(rows[0][1]).toStrictEqual(longMap2);
        expect(rows[0].get(1)).toStrictEqual(longMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(longMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(longMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(longMap);
            expect(rows2[0].get(0)).toStrictEqual(longMap);
            expect(rows2[0][1]).toStrictEqual(longMap2);
            expect(rows2[0].get(1)).toStrictEqual(longMap2);
          });
        });
      });
  }));
});

test("float/float map", async () => {
  const spark = await sharedSpark;
  const floatMap = new Map([[1.0, 2.0], [3.0, 4.0]]);
  const floatMap2 = new Map([[5.0, 6.0], [7.0, 8.0]]);
  const floatMapType = DataTypes.createMapType(DataTypes.FloatType, DataTypes.FloatType, false);
  return (
    spark.sql("SELECT map(1.0f, 2.0f, 3.0f, 4.0f) a, map(5.0f, 6.0f, 7.0f, 8.0f) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(floatMap);
        expect(rows[0].get(0)).toStrictEqual(floatMap);
        expect(rows[0][1]).toStrictEqual(floatMap2);
        expect(rows[0].get(1)).toStrictEqual(floatMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(floatMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(floatMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(floatMap);
            expect(rows2[0].get(0)).toStrictEqual(floatMap);
            expect(rows2[0][1]).toStrictEqual(floatMap2);
            expect(rows2[0].get(1)).toStrictEqual(floatMap2);
          });
        });
      });
  }));
});


test("double/double map", async () => {
  const spark = await sharedSpark;
  const doubleMap = new Map([[1.0, 2.0], [3.0, 4.0]]);
  const doubleMap2 = new Map([[5.0, 6.0], [7.0, 8.0]]);
  const doubleMapType = DataTypes.createMapType(DataTypes.DoubleType, DataTypes.DoubleType, false);
  return (
    spark.sql("SELECT map(1.0d, 2.0d, 3.0d, 4.0d) a, map(5.0d, 6.0d, 7.0d, 8.0d) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(doubleMap);
        expect(rows[0].get(0)).toStrictEqual(doubleMap);
        expect(rows[0][1]).toStrictEqual(doubleMap2);
        expect(rows[0].get(1)).toStrictEqual(doubleMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(doubleMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(doubleMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(doubleMap);
            expect(rows2[0].get(0)).toStrictEqual(doubleMap);
            expect(rows2[0][1]).toStrictEqual(doubleMap2);
            expect(rows2[0].get(1)).toStrictEqual(doubleMap2);
          });
        });
      });
  }));
});

test("decimal/decimal map", async () => {
  const spark = await sharedSpark;
  const decimalMap = new Map([[1.1, 2.1], [3.1, 4.1]]);
  const decimalMap2 = new Map([[5.1, 6.1], [7.1, 8.1]]);
  const decimalMapType = DataTypes.createMapType(DataTypes.createDecimalType(2, 1), DataTypes.createDecimalType(2, 1), false);
  return (
    spark.sql("SELECT map(1.1bd, 2.1bd, 3.1bd, 4.1bd) a, map(5.1bd, 6.1bd, 7.1bd, 8.1bd) b").then(df => {
      return df.collect().then(rows => {
        expect(getAsPlainJS(decimalMapType, rows[0][0])).toStrictEqual(decimalMap);
        expect(rows[0].get(0)).toStrictEqual(decimalMap);
        expect(getAsPlainJS(decimalMapType, rows[0][1])).toStrictEqual(decimalMap2);
        expect(rows[0].get(1)).toStrictEqual(decimalMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(decimalMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(decimalMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(getAsPlainJS(decimalMapType, rows2[0][0])).toStrictEqual(decimalMap);
            expect(rows2[0].get(0)).toStrictEqual(decimalMap);
            expect(getAsPlainJS(decimalMapType, rows2[0][1])).toStrictEqual(decimalMap2);
            expect(rows2[0].get(1)).toStrictEqual(decimalMap2);
          });
        });
      });
  }));
});

test("string/string map", async () => {
  const spark = await sharedSpark;
  const stringMap = new Map([["a", "b"], ["c", "d"]]);
  const stringMap2 = new Map([["e", "f"], ["g", "h"]]);
  const stringMapType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false);
  return (
    spark.sql("SELECT map('a', 'b', 'c', 'd') a, map('e', 'f', 'g', 'h') b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(stringMap);
        expect(rows[0].get(0)).toStrictEqual(stringMap);
        expect(rows[0][1]).toStrictEqual(stringMap2);
        expect(rows[0].get(1)).toStrictEqual(stringMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(stringMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(stringMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(stringMap);
            expect(rows2[0].get(0)).toStrictEqual(stringMap);
            expect(rows2[0][1]).toStrictEqual(stringMap2);
            expect(rows2[0].get(1)).toStrictEqual(stringMap2);
          });
        });
      });
  }));
});

test("binary/binary map", async () => {
  const spark = await sharedSpark;
  const buffer1 = Buffer.from("apache");
  const buffer2 = Buffer.from("spark");
  const uint8Array1 = new Uint8Array(buffer1);
  const uint8Array2 = new Uint8Array(buffer2);
  const binaryMap = new Map([[uint8Array1, uint8Array2]]);
  const binaryMap2 = new Map([[uint8Array2, uint8Array1]]);
  const binaryMapType = DataTypes.createMapType(DataTypes.BinaryType, DataTypes.BinaryType, false);
  return (
    spark.sql("SELECT map(cast('apache' as binary), cast('spark' as binary)) a, map(cast('spark' as binary), cast('apache' as binary)) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(binaryMap);
        expect(rows[0].get(0)).toStrictEqual(binaryMap);
        expect(rows[0][1]).toStrictEqual(binaryMap2);
        expect(rows[0].get(1)).toStrictEqual(binaryMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(binaryMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(binaryMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(binaryMap);
            expect(rows2[0].get(0)).toStrictEqual(binaryMap);
            expect(rows2[0][1]).toStrictEqual(binaryMap2);
            expect(rows2[0].get(1)).toStrictEqual(binaryMap2);
          });
        });
      });
  }));
});

test("date/date map", async () => {
  const spark = await sharedSpark;
  const dateMap = new Map([[new Date("2021-01-01"), new Date("2018-11-17")], [new Date("2020-12-31"), new Date("2021-01-01")]]);
  const dateMap2 = new Map([[new Date("2020-12-31"), new Date("2021-01-01")], [new Date("2018-11-17"), new Date("2021-01-01")]]);
  const dateMapType = DataTypes.createMapType(DataTypes.DateType, DataTypes.DateType, false);
  return (
    spark.sql("SELECT map(date'2021-01-01', date'2018-11-17', date'2020-12-31', date'2021-01-01') a, map(date'2020-12-31', date'2021-01-01', date'2018-11-17', date'2021-01-01') b").then(df => {
      return df.collect().then(rows => {
        expect(getAsPlainJS(dateMapType, rows[0][0])).toStrictEqual(dateMap);
        expect(rows[0].get(0)).toStrictEqual(dateMap);
        expect(getAsPlainJS(dateMapType, rows[0][1])).toStrictEqual(dateMap2);
        expect(rows[0].get(1)).toStrictEqual(dateMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(dateMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(dateMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(getAsPlainJS(dateMapType, rows2[0][0])).toStrictEqual(dateMap);
            expect(rows2[0].get(0)).toStrictEqual(dateMap);
            expect(getAsPlainJS(dateMapType, rows2[0][1])).toStrictEqual(dateMap2);
            expect(rows2[0].get(1)).toStrictEqual(dateMap2);
          });
        });
      });
  }));
});

test("timestamp/timestamp map", async () => {
  const spark = await sharedSpark;
  const timezone =  await spark.sql("SELECT timestamp'2021-01-01 01:00:00 Z' - timestamp'2021-01-01 01:00:00'").then(df => df.head()).then(row => row.get(0));
  const timezoneNumber = Number(timezone / 1000n);
  const timestampMap = new Map([[new Date("2021-01-01 01:00:00Z"), new Date("2018-11-17 13:33:33Z")], [new Date("2020-12-31 23:59:59Z"), new Date("2021-01-01 01:00:00Z")]]);
  const timestampMap2 = new Map([[new Date("2020-12-31 23:59:59Z"), new Date("2021-01-01 01:00:00Z")], [new Date("2018-11-17 13:33:33Z"), new Date("2021-01-01 01:00:00Z")]]);
  const timestampMap3 = new Map();
  timestampMap.forEach((value, key) => {
    timestampMap3.set(new Date(Number(key) - timezoneNumber), new Date(Number(value) - timezoneNumber));
  });
  const timestampMap4 = new Map();
  timestampMap2.forEach((value, key) => {
    timestampMap4.set(new Date(Number(key) - timezoneNumber), new Date(Number(value) - timezoneNumber));
  });

  const df = await spark.sql("SELECT map(timestamp'2021-01-01 01:00:00', timestamp'2018-11-17 13:33:33', timestamp'2020-12-31 23:59:59', timestamp'2021-01-01 01:00:00') a, map(timestamp'2020-12-31 23:59:59', timestamp'2021-01-01 01:00:00', timestamp'2018-11-17 13:33:33', timestamp'2021-01-01 01:00:00') b")
  const row = await df.head();
  const timestampMapType = DataTypes.createMapType(DataTypes.TimestampType, DataTypes.TimestampType, false);

  expect(getAsPlainJS(timestampMapType, row.get(0))).toStrictEqual(timestampMap3);
  expect(row.get(0)).toStrictEqual(timestampMap3);
  expect(getAsPlainJS(timestampMapType, row.get(1))).toStrictEqual(timestampMap4);
  expect(row.get(1)).toStrictEqual(timestampMap4);
  const df2 = spark.createDataFrame([row], await df.schema());
  const row2 = await df2.head();
  expect(getAsPlainJS(timestampMapType, row2.get(0))).toStrictEqual(timestampMap3);
  expect(row2.get(0)).toStrictEqual(timestampMap3);
  expect(getAsPlainJS(timestampMapType, row2.get(1))).toStrictEqual(timestampMap4);
  expect(row2.get(1)).toStrictEqual(timestampMap4);
});

test("array map", async () => {
  const spark = await sharedSpark;
  const arrayMap = new Map();
  arrayMap.set([1, 2], [3, 4]);
  const arrayMap2 = new Map();
  arrayMap2.set([5, 6], [7, 8]);
  const arrayMapType = DataTypes.createMapType(DataTypes.createArrayType(DataTypes.IntegerType, false), DataTypes.createArrayType(DataTypes.IntegerType, false), false);
  return (
    spark.sql("SELECT map(array(1, 2), array(3, 4)) a, map(array(5, 6), array(7, 8)) b").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(arrayMap);
        expect(rows[0].get(0)).toStrictEqual(arrayMap);
        expect(rows[0][1]).toStrictEqual(arrayMap2);
        expect(rows[0].get(1)).toStrictEqual(arrayMap2);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(arrayMapType);
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect(schema.fields[1].dataType).toStrictEqual(arrayMapType);
          expect(schema.fields[1].name).toBe("b");
          expect(schema.fields[1].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(arrayMap);
            expect(rows2[0].get(0)).toStrictEqual(arrayMap);
            expect(rows2[0][1]).toStrictEqual(arrayMap2);
            expect(rows2[0].get(1)).toStrictEqual(arrayMap2);
          });
        });
      });
  }));
});

test("map value with nulls", async () => {
  const spark = await sharedSpark;
  return (
    spark.sql("SELECT map(1, 2, 3, null) a").then(df => {
      return df.collect().then(rows => {
        expect(rows[0][0]).toStrictEqual(new Map([[1, 2], [3, null]]));
        expect(rows[0].get(0)).toStrictEqual(new Map([[1, 2], [3, null]]));
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toStrictEqual(DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType, true));
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(new Map([[1, 2], [3, null]]));
            expect(rows2[0].get(0)).toStrictEqual(new Map([[1, 2], [3, null]]));
          });
        });
      });
  }));
});

test("struct", async () => {
  const spark = await sharedSpark;
  const struct = {
    a: true,
    b: 1,
    c: 2,
    d: 3,
    e: 4n,
    f: 5.0,
    g: 6.0,
    h: bigintToDecimalBigNum(-789n),
    i: "hello",
    j: new Uint8Array(Buffer.from("world")),
    k: new Date("2021-01-01").getTime(),
    l: new Date("2021-01-01 01:00:00Z").getTime(),
    m: [1, 2, 3],
    n: new Map([[1, 2], [3, 4]]),
    o: {a: 1, b: 2},
    p: {c: 3, d: 4},
    q: null
  };
  const result = {
    a: true,
    b: 1,
    c: 2,
    d: 3,
    e: 4n,
    f: 5.0,
    g: 6.0,
    h: -7.89,
    i: "hello",
    j: new Uint8Array(Buffer.from("world")),
    k: new Date("2021-01-01"),
    l: new Date("2021-01-01 01:00:00Z"),
    m: [1, 2, 3],
    n: new Map([[1, 2], [3, 4]]),
    o: {a: 1, b: 2},
    p: {c: 3, d: 4},
    q: null
  };
  return (
    spark.sql("SELECT named_struct(" +
      "'a', true, " +
      "'b', 1Y, " +
      "'c', 2S, " +
      "'d', 3, " +
      "'e', 4L, " +
      "'f', 5.0f, " +
      "'g', 6.0d, " +
      "'h', -7.89bd, " +
      "'i', 'hello', " +
      "'j', cast('world' as binary), " +
      "'k', date'2021-01-01', " +
      "'l', timestamp'2021-01-01 01:00:00Z', " +
      "'m', array(1, 2, 3), " +
      "'n', map(1, 2, 3, 4), " +
      "'o', named_struct('a', 1, 'b', 2), " +
      "'p', named_struct('c', 3, 'd', 4), " +
      "'q', null) a").then(async df => {
      return df.collect().then(async rows => {
        expect(rows[0][0]).toStrictEqual(struct);
        expect(rows[0].get(0)).toStrictEqual(result);
        return df.schema().then(schema => {
          expect(schema.fields[0].dataType).toBeInstanceOf(StructType)
          expect(schema.fields[0].name).toBe("a");
          expect(schema.fields[0].nullable).toBe(false);
          expect((schema.fields[0].dataType as StructType).fields[0].dataType).toBe(DataTypes.BooleanType);
          expect((schema.fields[0].dataType as StructType).fields[1].dataType).toBe(DataTypes.ByteType);
          expect((schema.fields[0].dataType as StructType).fields[2].dataType).toBe(DataTypes.ShortType);
          expect((schema.fields[0].dataType as StructType).fields[3].dataType).toBe(DataTypes.IntegerType);
          expect((schema.fields[0].dataType as StructType).fields[4].dataType).toBe(DataTypes.LongType);
          expect((schema.fields[0].dataType as StructType).fields[5].dataType).toBe(DataTypes.FloatType);
          expect((schema.fields[0].dataType as StructType).fields[6].dataType).toBe(DataTypes.DoubleType);
          expect((schema.fields[0].dataType as StructType).fields[7].dataType).toStrictEqual(DataTypes.createDecimalType(3, 2));
          expect((schema.fields[0].dataType as StructType).fields[8].dataType).toBe(DataTypes.StringType);
          expect((schema.fields[0].dataType as StructType).fields[9].dataType).toBe(DataTypes.BinaryType);
          expect((schema.fields[0].dataType as StructType).fields[10].dataType).toBe(DataTypes.DateType);
          expect((schema.fields[0].dataType as StructType).fields[11].dataType).toBe(DataTypes.TimestampType);
          expect((schema.fields[0].dataType as StructType).fields[12].dataType).toStrictEqual(DataTypes.createArrayType(DataTypes.IntegerType, false));
          expect((schema.fields[0].dataType as StructType).fields[13].dataType).toStrictEqual(DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType, false));
          expect((schema.fields[0].dataType as StructType).fields[14].dataType).toBeInstanceOf(StructType);
          expect((schema.fields[0].dataType as StructType).fields[15].dataType).toBeInstanceOf(StructType);
          expect((schema.fields[0].dataType as StructType).fields[16].dataType).toBe(DataTypes.NullType);
          return spark.createDataFrame(rows, schema).collect().then(rows2 => {
            expect(rows2[0][0]).toStrictEqual(struct);
            expect(rows2[0].get(0)).toStrictEqual(result);
          });
        });
      });
  }));
});
