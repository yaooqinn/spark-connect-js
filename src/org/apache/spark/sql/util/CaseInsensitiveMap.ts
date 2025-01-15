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

export class CaseInsensitiveMap<V> extends Map<string, V> {
  constructor(entries?: Iterable<readonly [string, V]> | null) {
    super(entries);
  }

  get(key: string): V | undefined {
    return super.get(key.toLowerCase());
  }

  set(key: string, value: V): this {
    return super.set(key.toLowerCase(), value);
  }

  has(key: string): boolean {
    return super.has(key.toLowerCase());
  }

  delete(key: string): boolean {
    return super.delete(key.toLowerCase());
  }

  forEach(callbackfn: (value: V, key: string, map: Map<string, V>) => void, thisArg?: any): void {
    super.forEach((value, key, map) => {
      callbackfn(value, key.toLowerCase(), map);
    }, thisArg);
  }

  *entries(): IterableIterator<[string, V]> {
    for (const [key, value] of super.entries()) {
      yield [key.toLowerCase(), value];
    }
  }

  *keys(): IterableIterator<string> {
    for (const key of super.keys()) {
      yield key.toLowerCase();
    }
  }

  *values(): IterableIterator<V> {
    for (const value of super.values()) {
      yield value;
    }
  }

  toIndexSignature(): { [key: string]: V } {
    let obj: { [key: string]: V } = {};
    for (const [key, value] of this.entries()) {
      obj[key] = value;
    }
    return obj;
  }
}