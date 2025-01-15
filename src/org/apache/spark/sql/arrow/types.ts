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

import { Bool, DateDay, Decimal, Duration, Float32, Float64, Int16, Int32, Int64, Int8, Interval, IntervalUnit, LargeBinary, LargeUtf8, Null, TimestampMicrosecond, TimeUnit } from "apache-arrow";

export const ARROW_NULL = new Null();
export const ARROW_BOOL = new Bool();
export const ARROW_INT8 = new Int8();
export const ARROW_INT16 = new Int16();
export const ARROW_INT32 = new Int32();
export const ARROW_INT64 = new Int64();
export const ARROW_FLOAT32 = new Float32();
export const ARROW_FLOAT64 = new Float64();
export const ARROW_UTF8 = new LargeUtf8();
export const ARROW_BINARY = new LargeBinary();
export const ARROW_DATE = new DateDay();
// Make TZ settable?
export const ARROW_TIMESTAMP = new TimestampMicrosecond(new Intl.DateTimeFormat().resolvedOptions().timeZone)
export const ARROW_TIMESTAMP_NTZ = new TimestampMicrosecond();
export const ARROW_INTERVAL = new Interval(IntervalUnit.MONTH_DAY_NANO);
export const ARROW_INTERVAL_DT = new Duration(TimeUnit.MICROSECOND);
export const ARROW_INTERVAL_YM = new Interval(IntervalUnit.YEAR_MONTH)
export const createArrowDecimal = (precision: number, scale: number) => new Decimal(scale, precision);

