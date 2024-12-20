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

import log4js from 'log4js';
import fs from 'fs';
import path from 'path';

let logger: Logger;

interface Logger {
  trace: (message?: any, ...optionalParams: any[]) => void;
  debug: (message?: any, ...optionalParams: any[]) => void;
  info: (message?: any, ...optionalParams: any[]) => void;
  warn: (message?: any, ...optionalParams: any[]) => void;
  error: (message?: any, ...optionalParams: any[]) => void;
  fatal: (message?: any, ...optionalParams: any[]) => void;
}

const configFilePath = path.join(process.cwd(), 'log4js.json');

const defaultConfig = {
  appenders: {
    stdout: { type: "stdout", layout: { type: "colored" } },
    file: {
      type: 'file',
      filename: 'logs/tspark-connect',
      pattern: '-yyyy-MM-dd.log',
      alwaysIncludePattern: true,
      layout: { type: "pattern", pattern: "%d{yyyy-MM-dd hh:mm:ss.SSS} [%p] %c - %m%n" }
    }
  },
  categories: {
    default: { appenders: ['stdout', 'file'], level: 'debug' }
  }
};

const configureLogger = () => {
  if (fs.existsSync(configFilePath)) {
    try {
      const customConfig = require(configFilePath);
      log4js.configure(customConfig);
      console.info('Log4js configured using the provided configuration file.');
    } catch (error) {
      console.error('Failed to load log4js configuration from file:', error);
      log4js.configure(defaultConfig);
      console.warn('Falling back to default Log4js configuration.');
    }
  } else {
    log4js.configure(defaultConfig);
    console.info('Using default Log4js configuration.');
  }
};

const initializeLogger = () => {
  configureLogger();
  logger = log4js.getLogger();
};

initializeLogger();

export { logger };