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

export class Table {
  constructor(
    public readonly name: string,
    public readonly catalog: string | null,
    public readonly namespace: string[] | null,
    public readonly description: string | null,
    public readonly tableType: string,
    public readonly isTemporary: boolean) {}

  get database(): string | null {
    return this.namespace != null && this.namespace.length === 1 ? this.namespace[0] : null;
  }

  toString(): string {
    const catalogStr = this.catalog == null ? "" : `catalog=${this.catalog}`;
    const databaseStr = this.database == null ? "" : `database=${this.namespace}`;
    const descriptionStr = this.description == null ? "" : `description=${this.description}`;
    return `Table(name=${this.name}, ${catalogStr}, ${databaseStr}, ${descriptionStr}, ` +
      `tableType=${this.tableType}, isTemporary=${this.isTemporary})`;
  }
}