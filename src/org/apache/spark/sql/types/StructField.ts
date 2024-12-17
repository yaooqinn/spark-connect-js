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

import * as t from "../../../../../gen/spark/connect/types_pb";
import { DataType } from "./data_types";
import { Metadata } from "./metadata";
import { DataTypes } from "./DataTypes";

/**
 * A field inside a StructType.
 * @param {string} name - The name of this field.
 * @param {DataType} dataType - The data type of this field.
 * @param {boolean} nullable - Indicates if values of this field can be `null` values.
 * @param {Metadata} metadata
 *   The metadata of this field. The metadata should be preserved during transformation if the
 *   content of the column is not modified, e.g, in selection.
 *
 * @since 1.0.0
 * @author Kent Yao
 */
export class StructField {
  name: string;
  dataType: DataType;
  nullable: boolean
  metadata: Metadata;

  constructor(name: string, dataType: DataType, nullable: boolean = true, metadata: Metadata = Metadata.empty()) {
    this.name = name;
    this.dataType = dataType;
    this.nullable = nullable;
    this.metadata = metadata;
  }

  toString(): string {
    return `StructField(${this.name},${this.dataType.toString()},${this.nullable})`;
  }

  getComment(): string | undefined {
    return this.metadata.contains("comment") ? this.metadata.getString("comment") : undefined;
  }

  private getDDLComment(): string {
    const comment = this.getComment();
    return comment ? ` COMMENT '${comment.replace(/'/g, "\\'")}'` : "";
  }

  private getDDLNull(): string {
    return this.nullable ? "" : " NOT NULL";
  }
  
  sql(): string {
    // TODO: quoting the name needed?
    return `${this.name}: ${this.dataType.sql()}${this.getDDLComment()}${this.getDDLNull()}`;
  }

  static fromProto(proto: t.DataType_StructField): StructField {
    if (!proto.dataType) {
      throw new Error("StructField data type is null");
    } else {
      const dataType = DataTypes.fromProto(proto.dataType);
      const metadata = proto.metadata ? Metadata.fromJson(proto.metadata) : undefined;
      return new StructField(proto.name, dataType, proto.nullable, metadata);
    }
  }
}