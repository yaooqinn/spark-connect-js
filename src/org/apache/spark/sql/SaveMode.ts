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

import * as c from "../../../../gen/spark/connect/commands_pb"

/**
 * SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
 * 
 * @stable
 * @since 1.0.0
 * @see [[DataFrameWriter]]
 * @see https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/SaveMode.html
 * @author Kent Yao <yao@apache.org>
 */
export enum SaveMode {
  /**
   * Append mode means that when saving a DataFrame to a data source, if data/table already exists,
   * contents of the DataFrame are expected to be appended to existing data.
   */
  Append = c.WriteOperation_SaveMode.APPEND,
  /**
   * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
   * an exception is expected to be thrown.
   */
  ErrorIfExists = c.WriteOperation_SaveMode.ERROR_IF_EXISTS,
  /**
   * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
   * the save operation is expected to not save the contents of the DataFrame and to not change
   * the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.
   */
  Ignore = c.WriteOperation_SaveMode.IGNORE,
  /**
   * Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists,
   * existing data is expected to be overwritten by the contents of the DataFrame.
   */
  Overwrite = c.WriteOperation_SaveMode.OVERWRITE
}