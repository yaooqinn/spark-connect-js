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

export class Column {
  constructor(
    public readonly name: string,
    public readonly description: string | null,
    public readonly dataType: string,
    public readonly nullable: boolean,
    public readonly isPartition: boolean,
    public readonly isBucket: boolean,
    public readonly isCluster: boolean = false) {}

  toString(): string {
    return `Column[name='${this.name}', ` +
      `${this.description ? `description='${this.description}', ` : ""}` +
      `dataType='${this.dataType}', ` +
      `nullable='${this.nullable}', ` +
      `isPartition='${this.isPartition}', ` +
      `isBucket='${this.isBucket}', ` +
      `isCluster='${this.isCluster}']`;
  }
}