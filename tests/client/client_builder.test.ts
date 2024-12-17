import { ClientBuilder } from "../../src/client/client_builder";

test("connection string to builder", () => {
    const builder = new ClientBuilder();
    builder.connectionString("sc://localhost:15002;user_id=yao;user_name=kent;session_id=6ec0bd7f-11c0-43da-975e-2a8ad9ebae0b;a=b;c=d");
    const conf = builder.conf();
    expect(conf).toBeDefined();
    const uc = conf.get_user_context();
    expect(uc.userId).toBe("yao");
    expect(uc.userName).toBe("kent");
    const metadata = conf.get_metadata().getMap();
    const expectedMeta = {"a": "b", "c": "d"}
    expect(metadata).toStrictEqual(expectedMeta);
});


test("invalid session id", () => {
    const builder = new ClientBuilder();
    expect(() => builder.connectionString("sc://localhost:15002;session_id=invalid")).toThrowError("Invalid session_id: invalid");
});
