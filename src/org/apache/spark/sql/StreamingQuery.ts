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
import * as cmd from '../../../../gen/spark/connect/commands_pb';
import { SparkSession } from './SparkSession';

/**
 * A handle to a streaming query that is executing continuously in the background.
 * 
 * @since 1.0.0
 */
export class StreamingQuery {
  public readonly id: string;
  public readonly runId: string;
  public readonly name: string;

  constructor(
    private spark: SparkSession,
    id: string,
    runId: string,
    name?: string
  ) {
    this.id = id;
    this.runId = runId;
    this.name = name || "";
  }

  /**
   * Waits for the termination of this query, either by query.stop() or by an exception.
   * If the query has terminated with an exception, then the exception will be thrown.
   * 
   * @param timeout optional timeout in milliseconds
   * @returns true if the query has terminated, false if timeout was reached
   */
  async awaitTermination(timeout?: number): Promise<boolean> {
    const awaitCmd = create(cmd.StreamingQueryCommand_AwaitTerminationCommandSchema, {
      timeoutMs: timeout !== undefined ? BigInt(timeout) : undefined
    });
    
    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "awaitTermination", value: awaitCmd }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    const resps = await this.spark.execute(command);
    const resp = resps.filter(r => r.isStreamingQueryCommandResult)[0];
    
    if (resp && resp.streamingQueryCommandResult) {
      const result = resp.streamingQueryCommandResult;
      if (result.awaitTermination) {
        return result.awaitTermination.terminated;
      }
    }
    return false;
  }

  /**
   * Stops the execution of this query.
   */
  async stop(): Promise<void> {
    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "stop", value: true }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    await this.spark.execute(command);
  }

  /**
   * Blocks until all available data in the source has been processed and committed to the sink.
   * This method is intended for testing.
   */
  async processAllAvailable(): Promise<void> {
    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "processAllAvailable", value: true }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    await this.spark.execute(command);
  }

  /**
   * Returns the current status of the query.
   */
  async status(): Promise<any> {
    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "status", value: true }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    const resps = await this.spark.execute(command);
    const resp = resps.filter(r => r.isStreamingQueryCommandResult)[0];
    
    if (resp && resp.streamingQueryCommandResult && resp.streamingQueryCommandResult.status) {
      return resp.streamingQueryCommandResult.status.statusMessage;
    }
    return null;
  }

  /**
   * Returns the most recent StreamingQueryProgress update of this streaming query.
   */
  async lastProgress(): Promise<any> {
    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "lastProgress", value: true }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    const resps = await this.spark.execute(command);
    const resp = resps.filter(r => r.isStreamingQueryCommandResult)[0];
    
    if (resp && resp.streamingQueryCommandResult && resp.streamingQueryCommandResult.recentProgress) {
      const progress = resp.streamingQueryCommandResult.recentProgress.recentProgressJson;
      return progress.length > 0 ? progress[progress.length - 1] : null;
    }
    return null;
  }

  /**
   * Returns an array of the most recent StreamingQueryProgress updates.
   */
  async recentProgress(): Promise<any[]> {
    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "recentProgress", value: true }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    const resps = await this.spark.execute(command);
    const resp = resps.filter(r => r.isStreamingQueryCommandResult)[0];
    
    if (resp && resp.streamingQueryCommandResult && resp.streamingQueryCommandResult.recentProgress) {
      return resp.streamingQueryCommandResult.recentProgress.recentProgressJson;
    }
    return [];
  }

  /**
   * Prints the physical and logical plans to the console for debugging purposes.
   */
  async explain(extended: boolean = false): Promise<void> {
    const explainCmd = create(cmd.StreamingQueryCommand_ExplainCommandSchema, {
      extended: extended
    });

    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "explain", value: explainCmd }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    const resps = await this.spark.execute(command);
    const resp = resps.filter(r => r.isStreamingQueryCommandResult)[0];
    
    if (resp && resp.streamingQueryCommandResult && resp.streamingQueryCommandResult.explain) {
      console.log(resp.streamingQueryCommandResult.explain.result);
    }
  }

  /**
   * Returns the exception if the query was terminated by an exception.
   */
  async exception(): Promise<any> {
    const queryCmd = create(cmd.StreamingQueryCommandSchema, {
      queryId: this.createQueryId(),
      command: { case: "exception", value: true }
    });

    const command = create(cmd.CommandSchema, {
      commandType: { case: "streamingQueryCommand", value: queryCmd }
    });

    const resps = await this.spark.execute(command);
    const resp = resps.filter(r => r.isStreamingQueryCommandResult)[0];
    
    if (resp && resp.streamingQueryCommandResult && resp.streamingQueryCommandResult.exception) {
      return resp.streamingQueryCommandResult.exception;
    }
    return null;
  }

  private createQueryId(): cmd.StreamingQueryInstanceId {
    return create(cmd.StreamingQueryInstanceIdSchema, {
      id: this.id,
      runId: this.runId
    });
  }
}
