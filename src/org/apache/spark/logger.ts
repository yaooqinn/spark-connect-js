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

interface Logger {
  trace: (message?: any, ...optionalParams: any[]) => void;
  debug: (message?: any, ...optionalParams: any[]) => void;
  info: (message?: any, ...optionalParams: any[]) => void;
  warn: (message?: any, ...optionalParams: any[]) => void;
  error: (message?: any, ...optionalParams: any[]) => void;
  fatal: (message?: any, ...optionalParams: any[]) => void;
}

class ConsoleLogger implements Logger {
  private prefix = '[spark-connect]';

  trace(message?: any, ...optionalParams: any[]): void {
    console.debug(this.prefix, '[TRACE]', message, ...optionalParams);
  }

  debug(message?: any, ...optionalParams: any[]): void {
    console.debug(this.prefix, '[DEBUG]', message, ...optionalParams);
  }

  info(message?: any, ...optionalParams: any[]): void {
    console.info(this.prefix, '[INFO]', message, ...optionalParams);
  }

  warn(message?: any, ...optionalParams: any[]): void {
    console.warn(this.prefix, '[WARN]', message, ...optionalParams);
  }

  error(message?: any, ...optionalParams: any[]): void {
    console.error(this.prefix, '[ERROR]', message, ...optionalParams);
  }

  fatal(message?: any, ...optionalParams: any[]): void {
    console.error(this.prefix, '[FATAL]', message, ...optionalParams);
  }
}

const logger = new ConsoleLogger();

export { logger };