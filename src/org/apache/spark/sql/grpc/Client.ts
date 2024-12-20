import { ClientBuilder, Configuration } from "./client_builder";
import * as grpc from '@grpc/grpc-js';
import * as b from '../../../../../gen/spark/connect/base_pb'; // proto import: "spark/connect/base.proto"
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { logger } from '../../logger';
import { type MessageShape, type DescMessage } from '@bufbuild/protobuf';
  

export class Client {
  conf_: Configuration;
  client_: grpc.Client;
  session_id_: string;
  user_context_: b.UserContext;
  user_agent_: string = "spark-connect-js"; // TODO: What shall I set here?
  metadata_: grpc.Metadata;

  constructor(conf: Configuration) {
    this.conf_ = conf;
    this.client_ = this.conf_.new_client();
    this.session_id_ = this.conf_.get_session_id();
    this.user_context_ = this.conf_.get_user_context();
    this.metadata_ = this.conf_.get_metadata();
  }

  static builder(): ClientBuilder {
    return new ClientBuilder();
  }

  close(): void {
    try {
        this.client_.close();
    } catch (e) {
        console.error("Failed to close the underlying grpc client", e);
    }
  }

  private api(method: string): string {
    return `/spark.connect.SparkConnectService/${method}`;
  }

  private handleGrpcError(err: any): any {
    if (err instanceof Error && "code" in err && err.code === grpc.status.INTERNAL && "details" in err) {
      logger.error(err.details);
      new Error(err.details as string);
    } else {
      err;
    }
  }

  private runServiceCall<ReqType, RespType>(
      method: string,
      req: ReqType,
      ser: (req: ReqType) => Buffer,
      deser: (resp: Buffer) => RespType): Promise<RespType> {
    logger.debug("Sending unnary request by", method, req);
    return new Promise((resolve, reject) => {
      this.client_.makeUnaryRequest<ReqType, RespType>(
        this.api(method),
        ser,
        deser,
        req,
        this.metadata_,
        {}, // call options
        (err: grpc.ServiceError | null, resp?: RespType) => {
          if (err) {
            const handledErrr = this.handleGrpcError(err);
            logger.error("Errored calling ", method, handledErrr);
            reject(handledErrr);
          } else if (resp) {
            logger.debug("Received response by", method, JSON.stringify(resp));
            resolve(resp);
          } else {
            const msg = "No response or error received by " + method;
            logger.error(msg);
            reject(new Error(msg));
          }
        }
      )
    });
  };

  private runServerStreamCall<ReqType, RespType>(
      method: string,
      req: ReqType,
      ser: (req: ReqType) => Buffer,
      deser: (resp: Buffer) => RespType): Promise<RespType> {
    logger.debug("Sending stream request by", method, req);
    return new Promise((resolve, reject) => {
      this.client_.makeServerStreamRequest<ReqType, RespType>(
        this.api(method),
        ser,
        deser,
        req,
        this.metadata_,
        {}
      ).on("data", (data: RespType) => {
          logger.debug("Received data from", method, data);
          resolve(data);
        }
      ).on("error", (err: any) => {
          const handledErrr = this.handleGrpcError(err);
          logger.error("Errored calling", method, handledErrr);
          reject(handledErrr);
        }
      ).on("end", () => {
          logger.debug("End of", method);
        }
      );
    });
  };

  private serializer<Desc extends DescMessage>(desc: Desc) {
    return (msg: MessageShape<Desc>) => {
       return Buffer.from(toBinary(desc, msg));
    }
  }

  private deserializer<Desc extends DescMessage>(desc: Desc) {
    return (buf: Buffer) => {
      return fromBinary(desc, buf);
    }
  }

  async analyze(analysis: (req: b.AnalyzePlanRequest) => void): Promise<b.AnalyzePlanResponse> {
    const req = create(b.AnalyzePlanRequestSchema, {
      sessionId: this.session_id_,
      userContext: this.user_context_,
      clientType: this.user_agent_
    });
    analysis(req);
    return this.runServiceCall<b.AnalyzePlanRequest, b.AnalyzePlanResponse>(
      "AnalyzePlan",
      req,
      this.serializer(b.AnalyzePlanRequestSchema),
      this.deserializer(b.AnalyzePlanResponseSchema),
    );
  };

  async config(operation: b.ConfigRequest_Operation): Promise<b.ConfigResponse> {
    const req = create(b.ConfigRequestSchema, {
        sessionId: this.session_id_,
        userContext: this.user_context_,
        clientType: this.user_agent_,
        operation: operation
    });
    return this.runServiceCall<b.ConfigRequest, b.ConfigResponse>(
      "Config",
      req,
      this.serializer(b.ConfigRequestSchema),
      this.deserializer(b.ConfigResponseSchema),
    );
  };

  async execute(plan: b.Plan): Promise<b.ExecutePlanResponse> {
    const req = create(b.ExecutePlanRequestSchema, {
      sessionId: this.session_id_,
      userContext: this.user_context_,
      clientType: this.user_agent_,
      plan: plan,
      tags: [] // TODO: FIXME
    });

    return this.runServerStreamCall<b.ExecutePlanRequest, b.ExecutePlanResponse>(
      "ExecutePlan",
      req,
      this.serializer(b.ExecutePlanRequestSchema),
      this.deserializer(b.ExecutePlanResponseSchema),
    );
  };
}