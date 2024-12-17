import { ClientBuilder, Configuration } from "./client_builder";
import * as grpc from '@grpc/grpc-js';
import * as connect from '../gen/spark/connect/base_pb'; // proto import: "spark/connect/base.proto"
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';

export class Client {
    conf_: Configuration;
    client_: grpc.Client;
    session_id_: string;
    user_context_: connect.UserContext;
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

    anlyze(applyOperation: (req: connect.AnalyzePlanRequest) => void): Promise<connect.AnalyzePlanResponse> {
        const req = create(connect.AnalyzePlanRequestSchema, {
            sessionId: this.session_id_,
            userContext: this.user_context_,
            clientType: this.user_agent_
        });
        applyOperation(req);
        return new Promise((resolve, reject) => {
            this.client_.makeUnaryRequest<connect.AnalyzePlanRequest, connect.AnalyzePlanResponse>(
                this.api("AnalyzePlan"),
                (req: connect.AnalyzePlanRequest) => {
                    return Buffer.from(toBinary(connect.AnalyzePlanRequestSchema, req));
                },
                (resp: Buffer) => {
                    return fromBinary(connect.AnalyzePlanResponseSchema, resp);
                },
                req,
                this.metadata_,
                (err: grpc.ServiceError | null, resp?: connect.AnalyzePlanResponse) => {
                    if (err) {
                        reject(err);
                    } else if (resp) {
                        resolve(resp);
                    }
                }
            )
        });
    };

    config(operation: connect.ConfigRequest_Operation): Promise<connect.ConfigResponse> {
        const req = create(connect.ConfigRequestSchema, {
            sessionId: this.session_id_,
            userContext: this.user_context_,
            clientType: this.user_agent_,
            operation: operation
        });
        return new Promise((resolve, reject) => {
            this.client_.makeUnaryRequest<connect.ConfigRequest, connect.ConfigResponse>(
                this.api("Config"),
                (req: connect.ConfigRequest) => {
                    return Buffer.from(toBinary(connect.ConfigRequestSchema, req));
                },
                (resp: Buffer) => {
                    return fromBinary(connect.ConfigResponseSchema, resp);
                },
                req,
                this.metadata_,
                (err: grpc.ServiceError | null, resp?: connect.ConfigResponse) => {
                    if (err) {
                        reject(err);
                    } else if (resp) {
                        resolve(resp);
                    }
                }
            )
        });
    };

    execute(plan: connect.Plan): Promise<connect.ExecutePlanResponse> {
        const req = create(connect.ExecutePlanRequestSchema, {
            sessionId: this.session_id_,
            userContext: this.user_context_,
            clientType: this.user_agent_,
            plan: plan,
            tags: [] // TODO: FIXME
        });
        return new Promise((resolve, reject) => {
            this.client_.makeUnaryRequest<connect.ExecutePlanRequest, connect.ExecutePlanResponse>(
                this.api("ExecutePlan"),
                (req: connect.ExecutePlanRequest) => {
                    return Buffer.from(toBinary(connect.ExecutePlanRequestSchema, req));
                },
                (resp: Buffer) => {
                    return fromBinary(connect.ExecutePlanResponseSchema, resp);
                },
                req,
                this.metadata_,
                (err: grpc.ServiceError | null, resp?: connect.ExecutePlanResponse) => {
                    if (err) {
                        reject(err);
                    } else if (resp) {
                        resolve(resp);
                    }
                }
            )
        });
    }
}