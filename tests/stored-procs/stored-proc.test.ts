import { execSync } from "child_process";

// // sibedge/postgres-plv8

describe("stored-proc-tests", () => {
    let cwd: string;
    beforeAll(() => {
        cwd = process.cwd();
        process.chdir("tests/stored-procs/proc-test");

        execSync("npm install");
        execSync("npm run build");
        execSync("npx dbosc compile");
    })

    afterAll(() => {
        process.chdir(cwd);
    });

    test("foo", () => {


    });

});