import { create } from "@bufbuild/protobuf";
import { Client } from "../../../../client/Client";
import * as connect from "../../../../gen/spark/connect/base_pb";

export class RuntimeConfig {
    static CONNECTION_TIMEOUT = "spark.connect.typescript.connection.timeout";
    static CONNECTION_TIMEOUT_DEFAULT = "30000";
    private client_: Client;
    constructor(client: Client) {
        this.client_ = client;
    }
    
    async set(k: string, v: string): Promise<connect.ConfigResponse> {
        const setConf = create(connect.ConfigRequest_SetSchema, {
            pairs: [create(connect.KeyValueSchema, {key: k, value: v})]
        });
        return this.execute(op => {
            op.opType = {
                value: setConf,
                case: "set"
            }
        });
    };

    async setAll(m: Map<string, string>): Promise<connect.ConfigResponse> {
        const pairs = Array.from(m.entries()).map(([k, v]) => {
            return create(connect.KeyValueSchema, {key: k, value: v});
        });
        const setAllConf = create(connect.ConfigRequest_SetSchema, {
            pairs: pairs
        });
        return this.execute(op => {
            op.opType = {
                value: setAllConf,
                case: "set"
            }
        });
    }

    async unset(k: string): Promise<connect.ConfigResponse> {
        const unsetConf = create(connect.ConfigRequest_UnsetSchema, {
            keys: [k]
        });
        return this.execute(op => {
            op.opType = {
                value: unsetConf,
                case: "unset"
            }
        });
    }
    
    async getOption(k: string): Promise<string | undefined> {
        return this.get(k).then(v => {
            return v;
        }).catch(() => { 
            return undefined;
        });
    }

    async get(k: string, defaultVal?: string): Promise<string> {
        let resp: Promise<connect.ConfigResponse>;
        if (defaultVal) {
            const getConf = create(connect.ConfigRequest_GetWithDefaultSchema, {
                pairs: [create(connect.KeyValueSchema, {key: k, value: defaultVal})]
            })
            resp = this.execute(op => {
                op.opType = {
                    value: getConf,
                    case: "getWithDefault"
                }
            });
        } else {
            const getConf = create(connect.ConfigRequest_GetSchema, {
                keys: [k]
            });
            resp = this.execute(op => {
                op.opType = {
                    value: getConf,
                    case: "get"
                }
            });
        }
        return resp.then(r => {
            // FIXME: assert pair length === 1
            if (r.pairs[0].value) {
                return r.pairs[0].value;
            } else {
                throw new Error(`Config: ${k} not found`);
            }
        });
    }

    async getAll(): Promise<Map<string, string | undefined>> {
        const getAll = create(connect.ConfigRequest_GetAllSchema, {});
        return this.execute(op => {
            op.opType = {
                value: getAll,
                case: "getAll"
            }
        }).then(r => {
            const m = new Map<string, string | undefined>();
            r.pairs.forEach(p => {
                m.set(p.key, p.value);
            });
            return m;
        });
    }

    async isModifiable(k: string): Promise<boolean> {
        const isModifiable = create(connect.ConfigRequest_IsModifiableSchema, {
            keys: [k]
        });
        const resp = this.execute(op => {
            op.opType = {
                value: isModifiable,
                case: "isModifiable"
            }
        });
        const r = await resp;
        // FIXME: assert pair length
        if (r.pairs[0].value) {
            return r.pairs[0].value.toLowerCase() === "true";
        }
        return false;
    }

    private execute(func: (op: connect.ConfigRequest_Operation) => void): Promise<connect.ConfigResponse> {
        const cro = create(connect.ConfigRequest_OperationSchema, {})
        func(cro);
        const resp = this.client_.config(cro);
        return resp;
    }
}

