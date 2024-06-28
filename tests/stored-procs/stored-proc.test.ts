import { execSync } from "child_process";
import { parseConfigFile } from "../../src";
import { Client } from "pg";

describe("stored-proc-tests", () => {
    let cwd: string;

    beforeAll(async () => {
        cwd = process.cwd();
        process.chdir("tests/stored-procs/proc-test");

        const [config,] = parseConfigFile();
        const client = new Client({
            user: config.poolConfig.user,
            port: config.poolConfig.port,
            host: config.poolConfig.host,
            password: config.poolConfig.password,
            database: "postgres",
        });

        await client.connect();
        await client.query(`DROP DATABASE IF EXISTS ${config.poolConfig.database};`);
        await client.query(`DROP DATABASE IF EXISTS ${config.system_database};`);
        await client.end();

        execSync("npm install");
        execSync("npm run build");
        execSync("npx dbos migrate");
    })

    afterAll(() => {
        process.chdir(cwd);
    });

    test("npm run test", () => {
        execSync("npm run test", { env: process.env });
    });
});