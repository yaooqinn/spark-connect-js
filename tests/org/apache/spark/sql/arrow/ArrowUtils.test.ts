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

import { bigintToDecimalBigNum, bigNumToNumber } from "../../../../../../src/org/apache/spark/sql/arrow/ArrowUtils";
import { util as arrowUtils } from "apache-arrow"

const { BN } = arrowUtils;

test("bigint to arrow decimal big num", () => {
  expect(bigintToDecimalBigNum(0n)).toStrictEqual(BN.decimal(new Uint32Array([0, 0, 0, 0])));
  expect(bigintToDecimalBigNum(1n)).toStrictEqual(BN.decimal(new Uint32Array([1, 0, 0, 0])));
  expect(bigintToDecimalBigNum(2n)).toStrictEqual(BN.decimal(new Uint32Array([2, 0, 0, 0])));
  expect(bigintToDecimalBigNum(3n)).toStrictEqual(BN.decimal(new Uint32Array([3, 0, 0, 0])));
  expect(bigintToDecimalBigNum(4n)).toStrictEqual(BN.decimal(new Uint32Array([4, 0, 0, 0])));

  expect(bigintToDecimalBigNum(0xFFFFFFFFn)).toStrictEqual(BN.decimal(new Uint32Array([4294967295, 0, 0, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFn + 1n)).toStrictEqual(BN.decimal(new Uint32Array([0, 1, 0, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFn + 2n)).toStrictEqual(BN.decimal(new Uint32Array([1, 1, 0, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFn + 3n)).toStrictEqual(BN.decimal(new Uint32Array([2, 1, 0, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFn + 4n)).toStrictEqual(BN.decimal(new Uint32Array([3, 1, 0, 0])));

  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn)).toStrictEqual(BN.decimal(new Uint32Array([4294967295, 4294967295, 0, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn + 1n)).toStrictEqual(BN.decimal(new Uint32Array([0, 0, 1, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn + 2n)).toStrictEqual(BN.decimal(new Uint32Array([1, 0, 1, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn + 3n)).toStrictEqual(BN.decimal(new Uint32Array([2, 0, 1, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn + 4n)).toStrictEqual(BN.decimal(new Uint32Array([3, 0, 1, 0])));

  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn + 0xFFFFFFFFn + 2n)).toStrictEqual(BN.decimal(new Uint32Array([0, 1, 1, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn + 0xFFFFFFFFn + 3n)).toStrictEqual(BN.decimal(new Uint32Array([1, 1, 1, 0])));
  expect(bigintToDecimalBigNum(0xFFFFFFFFFFFFFFFFn + 0xFFFFFFFFn + 4n)).toStrictEqual(BN.decimal(new Uint32Array([2, 1, 1, 0])));

  expect(bigintToDecimalBigNum(99999999999999999999999999999999999999n)).toStrictEqual(
    BN.decimal(new Uint32Array([4294967295, 160047679, 1518781562, 1262177448])));

  expect(bigintToDecimalBigNum(-0n)).toStrictEqual(BN.decimal(new Uint32Array([0, 0, 0, 0])));
  expect(bigintToDecimalBigNum(-1n)).toStrictEqual(BN.decimal(new Uint32Array([4294967295, 4294967295, 4294967295, 4294967295])));
  expect(bigintToDecimalBigNum(-2n)).toStrictEqual(BN.decimal(new Uint32Array([4294967294, 4294967295, 4294967295, 4294967295])));
  expect(bigintToDecimalBigNum(-3n)).toStrictEqual(BN.decimal(new Uint32Array([4294967293, 4294967295, 4294967295, 4294967295])));
  expect(bigintToDecimalBigNum(-4n)).toStrictEqual(BN.decimal(new Uint32Array([4294967292, 4294967295, 4294967295, 4294967295])));

  expect(bigintToDecimalBigNum(-0xFFFFFFFFn)).toStrictEqual(BN.decimal(new Uint32Array([1, 4294967295, 4294967295, 4294967295])));
  expect(bigintToDecimalBigNum(-0xFFFFFFFFn - 1n)).toStrictEqual(BN.decimal(new Uint32Array([0, 4294967295, 4294967295, 4294967295])));
  expect(bigintToDecimalBigNum(-0xFFFFFFFFn - 2n)).toStrictEqual(BN.decimal(new Uint32Array([4294967295, 4294967294, 4294967295, 4294967295])));
  expect(bigintToDecimalBigNum(-0xFFFFFFFFn - 3n)).toStrictEqual(BN.decimal(new Uint32Array([4294967294, 4294967294, 4294967295, 4294967295])));
});

test("decimal big num to number", () => {
  const n = bigintToDecimalBigNum(BigInt("1".repeat(38)));
  expect(() => bigNumToNumber(n)).toThrow("1".repeat(38));
  expect(() => bigNumToNumber(n, 10)).toThrow("1".repeat(28));
  expect(() => bigNumToNumber(n, 20)).toThrow("1".repeat(18));
  expect(bigNumToNumber(n, 30)).toBeCloseTo(11111111.111111112);
  expect(bigNumToNumber(n, 35)).toBeCloseTo(111.11111111111112);
  expect(bigNumToNumber(n, 40)).toBeCloseTo(0.0011111111111111111);
  const n1 = BN.decimal(new Uint32Array([3, 2, 1, 0]));
  expect(() => bigNumToNumber(n1)).toThrow("18446744082299486211");
  expect(bigNumToNumber(n1, 10)).toBeCloseTo(1844674408.2299486);
  expect(bigNumToNumber(n1, 15)).toBeCloseTo(18446.744082299486);
  expect(bigNumToNumber(n1, 20)).toBeCloseTo(0.18446744082299486);
  expect(bigNumToNumber(n1, 25)).toBeCloseTo(0.0000018446744082299486);

  const negative = new BN(new Uint32Array([0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF]), true);
  expect(bigNumToNumber(negative)).toBe(-1);
  expect(bigNumToNumber(negative, 1)).toBe(-0.1);
  expect(bigNumToNumber(negative, 2)).toBe(-0.01);
  expect(bigNumToNumber(negative, 3)).toBe(-0.001);
  expect(bigNumToNumber(negative, 4)).toBe(-0.0001);
});
