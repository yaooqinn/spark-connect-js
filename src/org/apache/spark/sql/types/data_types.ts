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



export abstract class DataType {

  defaultSize(): number {
    throw new Error("Not implemented");
  }

  typeName(): string {
    // NOTE: do not call toString() here, as it may be overridden by subclasses
    return this.constructor.name.replace(/Type$/, "").replace(/UDT$/, "").toLowerCase();
  };

  simpleString(): string { return this.typeName(); };

  catalogString(): string { return this.simpleString(); };

  sql(): string {
    return this.simpleString().toUpperCase();
  };

  toString(): string {
    return this.constructor.name;
  }
}

export abstract class AtomicType extends DataType {}

abstract class NumericType extends AtomicType {}

export abstract class IntegralType extends NumericType {}

export abstract class FractionalType extends NumericType {}

export abstract class DatetimeType extends AtomicType {}

export abstract class AnsiIntervalType extends AtomicType {}
