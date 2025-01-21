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

type MetadataType = { [key: string]: any };

/**
 * An API derived from Apache Spark's Metadata class for storing column metadata. Double and Long are
 * treated as the same number type. The original Scala description can be found below:
 * 
 * Metadata is a wrapper over Map[String, Any] that limits the value type to simple ones: Boolean,
 * Long, Double, String, Metadata, Array[Boolean], Array[Long], Array[Double], Array[String], and
 * Array[Metadata]. JSON is used for serialization.
 *
 * The default constructor is private. User should use either [[MetadataBuilder]] or
 * `Metadata.fromJson()` to create Metadata instances.
 *
 * @param { MetadataType } an immutable map that stores the data
 * @since 1.0.0
 * @author  Kent Yao <yao@apache.org>

 */
export class Metadata {
  metadata: MetadataType;
  private static EMPTY = new Metadata({});

  constructor(metadata: MetadataType) {
    this.metadata = metadata;
  }

  contains(key: string): boolean {
    return key in this.metadata;
  }

  isEmpty(): boolean {
    return  Object.entries(this.metadata).length === 0;
  }

  getLong(key: string): number {
    const value = this.get(key);
    if (typeof value === 'number') {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a number`);
    }
  }

  getDouble(key: string): number {
    return this.getLong(key);
  }

  getBoolean(key: string): boolean {
    const value = this.get(key);
    if (typeof value === 'boolean') {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a boolean`);
    }
  }

  getString(key: string): string {
    const value = this.get(key);
    if (typeof value === 'string') {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a string`);
    }
  }

  getMetadata(key: string): Metadata {
    const value = this.get(key);
    if (typeof value === 'object') {
      return new Metadata(value);
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a Metadata`);
    }
  }

  getLongArray(key: string): number[] {
    const value = this.get(key);
    if (Array.isArray(value) && value.every((v) => typeof v === 'number')) {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a number array`);
    }
  }

  getDoubleArray(key: string): number[] {
    return this.getLongArray(key);
  }

  getBooleanArray(key: string): boolean[] {
    const value = this.get(key);
    if (Array.isArray(value) && value.every((v) => typeof v === 'boolean')) {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a boolean array`);
    }
  }

  getStringArray(key: string): string[] {
    const value = this.get(key);
    if (Array.isArray(value) && value.every((v) => typeof v === 'string')) {
      return value;
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a string array`);
    }
  }

  getMetadataArray(key: string): Metadata[] {
    const value = this.get(key);
    if (Array.isArray(value) && value.every((v) => typeof v === 'object')) {
      return value.map((m) => new Metadata(m));
    } else {
      throw new Error(`Value '${value}' for '${key}' is not a Metadata array`);
    }
  }

  get(key: string): any {
    const value = this.metadata[key];
    if (value === undefined) {
      throw new Error(`Key '${key}' not found`);
    } else {
      return value;
    }
  }

  json(): string {
    return JSON.stringify(this.metadata);
  }

  static fromJson(jsonStr: string): Metadata {
    return new Metadata(JSON.parse(jsonStr));
  }

  static empty(): Metadata {
    return Metadata.EMPTY;
  }

  get [Symbol.toStringTag]() {
    return this.json();
  }

}

export class MetadataBuilder {
  private metadata: Map<string, any>;

  constructor() {
    this.metadata = new Map<string, any>();
  }

  withMetadata(metadata: Metadata): MetadataBuilder {
    for (const key in metadata.metadata) {
      this.metadata.set(key, metadata.metadata[key]);
    }
    return this;
  }

  putNull(key: string): MetadataBuilder {
    return this.put(key, null);
  }

  putLong(key: string, value: number): MetadataBuilder {
    return this.put(key, value);
  }

  putDouble(key: string, value: number): MetadataBuilder {
    return this.put(key, value);
  }

  putBoolean(key: string, value: boolean): MetadataBuilder {
    return this.put(key, value);
  }

  putString(key: string, value: string): MetadataBuilder {
    return this.put(key, value);
  }

  putMetadata(key: string, value: Metadata): MetadataBuilder {
    return this.put(key, value.metadata);
  }

  putLongArray(key: string, value: number[]): MetadataBuilder {
    return this.put(key, value);
  }

  putDoubleArray(key: string, value: number[]): MetadataBuilder {
    return this.put(key, value);
  }

  putBooleanArray(key: string, value: boolean[]): MetadataBuilder {
    return this.put(key, value);
  }

  putStringArray(key: string, value: string[]): MetadataBuilder {
    return this.put(key, value);
  }

  putMetadataArray(key: string, value: Metadata[]): MetadataBuilder {
    return this.put(key, value.map((m) => m.metadata));
  }

  putAll(map: Map<string, any>): MetadataBuilder {
    map.forEach((value, key) => {
      this.metadata.set(key, value);
    });
    return this;
  }

  build(): Metadata {
    let meta: { [key: string]: any } = {};
    this.metadata.forEach((value: any, key: string) => {
      meta[key] = value;
    });

    return new Metadata(meta);
  }

  private put(key: string, value: any): MetadataBuilder {
    this.metadata.set(key, value)
    return this
  }

  remove(key: string): MetadataBuilder {
    this.metadata.delete(key);
    return this
  }
}