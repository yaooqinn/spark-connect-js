import { Client } from "../../../../../src/org/apache/spark/sql/grpc/Client";
import { RuntimeConfig } from "../../../../../src/org/apache/spark/sql/RuntimeConfig";

function withClient(f: (client: Client) => void) {
  const builder = Client.builder();
  builder.connectionString("sc://localhost:15002;user_id=yao;user_name=kent");
  const client = builder.build();
  f(client);
}

function waitOptionUnsetWithTimeout(config: RuntimeConfig, key: string, timeout: number): Promise<void> {
  return new Promise((resolve, reject) => {
    let interval = setInterval(() => {
      config.getOption(key).then(v => {
        if (v === undefined) {
          clearInterval(interval);
          resolve();
        }
      });
    }, 1000);
    setTimeout(() => {
      clearInterval(interval);
      reject("timeout");
    }, timeout);
  });
}

test('runtime config - set, unset, get', async () => {
  withClient(client => {
    const config = new RuntimeConfig(client);
    config.get("spark.executor.id").then(v => {
      expect(v).toBe("driver");
    });

    config.get("spark.master").then(v => {
      expect(v).toBe("local[*]");
    });

    config.set("spark.kent", "yao").then(() => {
      config.get("spark.kent").then(v => {
        expect(v).toBe("yao");
      });
    });
    config.unset("spark.kent.not.exist").then(() => {
      config.getOption("spark.kent.not.exist").then(v => {
        expect(v).toBeUndefined();
      });
      config.get("spark.kent.not.exist", "yao").then(v => {
        expect(v).toBe("yao");
      });
    });
    config.unset("spark.kent").then(() => {
      waitOptionUnsetWithTimeout(config, "spark.kent", 10000).then(() => {
        config.getOption("spark.kent").then(v => {
          expect(v).toBeUndefined();
        });
        config.get("spark.kent").catch(e => {
          expect(e).toBeDefined();
          expect(e.message).toMatch("SQL_CONF_NOT_FOUND");
        });
      });
    }).catch(e => {
      console.error("Failed to unset config", e);
      config.getOption("spark.kent").then(v => {
        expect(v).toBeUndefined();
      });
      config.get("spark.kent").catch(e => {
        expect(e).toBeDefined();
        expect(e.message).toMatch("SQL_CONF_NOT_FOUND");
      });
    });
  });
});

test("runtime config - get all configs", async () => {
  withClient(client => {
    const config = new RuntimeConfig(client);
    config.getAll().then(configs => {
      expect(configs.get("spark.executor.id")).toBe("driver");
      expect(configs.get("spark.master")).toBe("local[*]");
    });
  });
});

test("runtime config - is modifiable", async () => {
  withClient(client => {
    const config = new RuntimeConfig(client);
    config.isModifiable("spark.sql.warehouse.dir").then(v => {
      expect(v).toBe(false);
    });
    config.isModifiable("spark.executor.id").then(v => {
      expect(v).toBe(false);
    });
    config.isModifiable("spark.master").then(v => {
      expect(v).toBe(false);
    });
    config.isModifiable("spark.sql.ansi.enabled").then(v => {
      expect(v).toBe(true);
    });
  });
});