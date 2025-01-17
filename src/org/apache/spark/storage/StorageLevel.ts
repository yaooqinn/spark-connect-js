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

export class StorageLevel {
  constructor(
      public readonly useDisk: boolean,
      public readonly useMemory: boolean,
      public readonly useOffHeap: boolean, 
      public readonly deserialized: boolean,
      public readonly replication: number) {
  }

  static NONE = new StorageLevel(false, false, false, false, 1);
  static DISK_ONLY = new StorageLevel(true, false, false, false, 1);
  static DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2);
  static MEMORY_ONLY = new StorageLevel(false, true, false, true, 1);
  static MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2);
  static MEMORY_ONLY_SER = new StorageLevel(false, true, false, false, 1);
  static MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2);
  static MEMORY_AND_DISK = new StorageLevel(true, true, false, true, 1);
  static MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2);
  static MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false, 1);
  static MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2);
  static OFF_HEAP = new StorageLevel(false, false, true, true, 1);

  equals(other: StorageLevel): boolean {
    return this.useDisk === other.useDisk &&
      this.useMemory === other.useMemory &&
      this.useOffHeap === other.useOffHeap &&
      this.deserialized === other.deserialized &&
      this.replication === other.replication;
  }

}