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

import { create } from '@bufbuild/protobuf';
import * as grpc from '@grpc/grpc-js';
import { ChannelCredentials, Interceptor } from '@grpc/grpc-js';
import { v4 as uuidv4, validate as uuidValidate } from 'uuid';
import { UserContext, UserContextSchema } from '../../../../../gen/spark/connect/base_pb';
import { Client } from './Client';

export class ClientBuilder {

  private _conf = new Configuration("anonymous", "anonymous");

  connectionString(str: string): ClientBuilder {
    const [baseUrl, ...params] = str.split(";");
    // console.log(baseUrl);
    const url = new URL(baseUrl);
    if (url.protocol !== "sc:") {
      throw new Error(`Invalid protocol: ${url.protocol}, want: sc:`);
    }
    this._conf.host(url.hostname);
    if (url.port) {
      this._conf.port(parseInt(url.port));
    }
    params.forEach(param => {
      const [key, value] = param.split("=");
      switch (key) {
        case "user_id": this._conf.user_id(value);
          break;
        case "user_name": this._conf.user_name(value);
          break;
        case "token": this._conf.token(value);
          break;
        case "use_ssl": this._conf.is_ssl_enabled(value === "true");
          break;
        case "session_id":
          if (!uuidValidate(value)) {
            throw new Error(`Invalid session_id: ${value}`);
          }
          this._conf.set_session_id(value);
          break;
        case "grpc_max_message_size": this._conf.max_message_size(parseInt(value));
          break;
        default: this._conf.set_metadata(key, value);
        }
      }
    );
    return this;
  }

  conf(): Configuration {
    return this._conf;
  }

  build(): Client {
    return new Client(this._conf);
  }

}

export class Configuration {
  private _user_id: string;
  private _user_name: string;
  private _host: string;
  private _port: number;
  private _token: string | null;
  private _is_ssl_enabled: boolean = false;
  private _metadata: grpc.Metadata;
  private _use_reattachable_execute: boolean;
  private _interceptors: Interceptor[] = [
    
  ];
  private _session_id : string | null;
  private _max_message_size: number; 
  private _max_recursion_limit: number;

  constructor(
    user_id: string,
    user_name: string,
    host: string = "localhost",
    port: number = 15002,
    token: string | null = null,
    is_ssl_enabled: boolean = false,
    metadata: grpc.Metadata = new grpc.Metadata(),
    use_reattachable_execute: boolean = true,
    interceptors: Interceptor[] = [],
    session_id: string | null = null,
    max_message_size: number = 128 * 1024 * 1024,
    max_recursion_limit: number = 1024) {
    this._user_id = user_id;
    this._user_name = user_name;
    this._host = host;
    this._port = port;
    this._token = token;
    this._is_ssl_enabled = is_ssl_enabled;
    this._metadata = metadata;
    this._use_reattachable_execute = use_reattachable_execute;
    this._interceptors = this._interceptors.concat(interceptors);
    this._session_id = session_id;
    this._max_message_size = max_message_size;
    this._max_recursion_limit = max_recursion_limit;
  }

  user_id(user_id: string): void {
    this._user_id = user_id;
  }

  user_name(user_name: string): void {
    this._user_name = user_name;
  }

  host(host: string): void {
    this._host = host;
  }

  port(port: number): void {
    this._port = port;
  }

  token(token: string): void {
    this._token = token;
  }

  is_ssl_enabled(is_ssl_enabled: boolean): void {
    this._is_ssl_enabled = is_ssl_enabled;
  }

  set_session_id(session_id: string): void {
    this._session_id = session_id;
  }

  get_session_id(): string {
    if (!this._session_id) {
        this._session_id = uuidv4();
    }
    return this._session_id;
  }

  max_message_size(max_message_size: number): void {
    this._max_message_size = max_message_size;
  }

  set_metadata(key: string, value: string): void {
    this._metadata.set(key, value);
  }

  get_metadata(): grpc.Metadata {
    return this._metadata
  }

  get_user_context(): UserContext {
    let uc = create(UserContextSchema, {
        userId: this._user_id,
        userName: this._user_name
    });
    return uc;
  }

  credentials(): ChannelCredentials {
    if (this._is_ssl_enabled) {
        if (this._token) {
            return ChannelCredentials.createSsl(Buffer.from(this._token));
        } else {
            return ChannelCredentials.createSsl();
        }
    } else {
        return ChannelCredentials.createInsecure();
    }
  }

  new_client(): grpc.Client {
    const address = `${this._host}:${this._port}`;
    const client = new grpc.Client(address, this.credentials(), {});
    // console.log("Client created: ", client);
    return client;
  }
};