import { Pool, PoolClient, PoolConfig } from "pg";
import { DBOS, Scheduled, SchedulerMode, TestingRuntime, Transaction, TransactionContext, Workflow, WorkflowContext, WorkflowQueue } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { createInternalTestRuntime, TestingRuntimeImpl } from "../../src/testing/testing_runtime";
import { sleepms } from "../../src/utils";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { PostgresSystemDatabase } from "../../src/system_database";

type TestTransactionContext = TransactionContext<PoolClient>;

describe("scheduled-wf-tests-simple", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        DBOSSchedTestClass.reset(true);
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });

    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime(undefined, config);
    });

    afterEach(async () => {
        await testRuntime.destroy();
    }, 10000);
  
    test("wf-scheduled", async () => {
        // Make sure two functions with the same name in different classes are not interfering with each other.
        await sleepms(2500);
        expect(DBOSSchedDuplicate.nCalls).toBeGreaterThanOrEqual(2);
        expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2);
        expect(DBOSSchedTestClass.nTooEarly).toBe(0);
        expect(DBOSSchedTestClass.nTooLate).toBe(0);

        await testRuntime.deactivateEventReceivers();

        await sleepms(1000);
        expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(3);

    });
});

const q = new WorkflowQueue("schedQ", 1);

class DBOSSchedTestClass {
    static nCalls = 0;
    static nQCalls = 0;
    static nTooEarly = 0;
    static nTooLate = 0;
    static doSleep = true;

    static reset(doSleep: boolean) {
        DBOSSchedTestClass.nCalls = 0;
        DBOSSchedTestClass.nQCalls = 0;
        DBOSSchedTestClass.nTooEarly = 0;
        DBOSSchedTestClass.nTooLate = 0;
        DBOSSchedTestClass.doSleep = doSleep;
    }

    @Transaction({isolationLevel: "READ COMMITTED"})
    static async scheduledTxn(ctxt: TestTransactionContext) {
        DBOSSchedTestClass.nCalls++;
        await ctxt.client.query("SELECT 1");
    }

    @Scheduled({crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive})
    @Workflow()
    static async scheduledDefault(ctxt: WorkflowContext, schedTime: Date, startTime: Date) {
        await ctxt.invoke(DBOSSchedTestClass).scheduledTxn();
        if (schedTime.getTime() > startTime.getTime()) DBOSSchedTestClass.nTooEarly++;
        if (startTime.getTime() - schedTime.getTime() > 1500) DBOSSchedTestClass.nTooLate++;

        if (DBOSSchedTestClass.doSleep) {
            await ctxt.sleepms(2000);
        }
    }

    @Scheduled({crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive, queueName: q.name})
    @Workflow()
    static async scheduledDefaultQ(_ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
        ++DBOSSchedTestClass.nQCalls;
        return Promise.resolve();
    }

    // This should run every 30 minutes. Making sure the testing runtime can correctly exit within a reasonable time.
    @Scheduled({crontab: '*/30 * * * *'})
    @Workflow()
    static async scheduledLong(ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
        await ctxt.sleepms(100);
    }
}

class DBOSSchedDuplicate {
    static nCalls = 0;

    @Transaction({isolationLevel: "READ COMMITTED"})
    static async scheduledTxn(ctxt: TestTransactionContext) {
        DBOSSchedDuplicate.nCalls++;
        await ctxt.client.query("SELECT 1");
    }

    @Scheduled({crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive})
    @Workflow()
    static async scheduledDefault(ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
        await ctxt.invoke(DBOSSchedDuplicate).scheduledTxn();
        return Promise.resolve();
    }
}

class DBOSSchedTestClassOAOO {
    static nCalls = 0;

    static reset() {
        DBOSSchedTestClassOAOO.nCalls = 0;
    }

    @Scheduled({crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerInterval})
    @Workflow()
    static async scheduledDefault(_ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
        DBOSSchedTestClassOAOO.nCalls++;
        return Promise.resolve();
    }
}

describe("scheduled-wf-tests-oaoo", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });
  
    beforeEach(async () => {
    });
  
    afterEach(async () => {
    }, 10000);
  
    test("wf-scheduled-recover", async () => {
        DBOSSchedTestClassOAOO.reset();
        testRuntime = await createInternalTestRuntime(undefined, config);
        let startDownTime: number[] = [];
        try {
            await sleepms(3000);
            expect(DBOSSchedTestClassOAOO.nCalls).toBeGreaterThanOrEqual(2);
            expect(DBOSSchedTestClassOAOO.nCalls).toBeLessThanOrEqual(4);
            startDownTime = process.hrtime();
        }
        finally {
            await testRuntime.destroy();
        }
        await sleepms(5000);
        DBOSSchedTestClassOAOO.reset();
        testRuntime = await createInternalTestRuntime(undefined, config);
        try {
            await sleepms(3000);
            expect(DBOSSchedTestClassOAOO.nCalls).toBeGreaterThanOrEqual(7); // 3 new ones, 5 recovered ones, +/-
            const endDownTime = process.hrtime();
            const nShouldHaveHappened = endDownTime[0] - startDownTime[0] + 2;
            expect(DBOSSchedTestClassOAOO.nCalls).toBeLessThanOrEqual(nShouldHaveHappened);
        }
        finally {
            await testRuntime.destroy();
        }
    }, 15000);
});

describe("scheduled-wf-tests-when-active", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });
  
    beforeEach(async () => {
    });
  
    afterEach(async () => {
    }, 10000);
  
    test("wf-scheduled-recover", async () => {
        DBOSSchedTestClass.reset(false);
        testRuntime = await createInternalTestRuntime(undefined, config);
        try {
            await sleepms(3000);
            expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2);
            expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(4);
            expect(DBOSSchedTestClass.nQCalls).toBeGreaterThanOrEqual(1); // This has some delay, potentially...

            const wfs = await testRuntime.getWorkflows({
                workflowName: "scheduledDefaultQ",
            });

            let foundQ = false;
            for (const wfid of wfs.workflowUUIDs) {
                const stat = await testRuntime.retrieveWorkflow(wfid).getStatus();
                if (stat?.queueName === q.name) foundQ = true;
            }
            expect (foundQ).toBeTruthy();
        }
        finally {
            await testRuntime.destroy();
        }
        await sleepms(5000);
        DBOSSchedTestClass.reset(false);
        testRuntime = await createInternalTestRuntime(undefined, config);
        try {
            await sleepms(3000);
            expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2); // 3 new ones, +/- 1
            expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(4); // No old ones from recovery or sleep interval
        }
        finally {
            await testRuntime.destroy();
        }
    }, 15000);
});

export async function simulateDbRestart(currentDb: string, downtime: number, baseConfig: PoolConfig, basePool: Pool): Promise<void> {
    const mainClient: PoolClient = await basePool.connect();

    try {
        // Get DB name
        const currentDbResult = await mainClient.query<{current_database: string}>("SELECT current_database()");
        const currentDb = currentDbResult.rows[0].current_database;
        const tempDbName = "temp_database_for_maintenance";

        // Create a temporary database
        try {
            await mainClient.query(`CREATE DATABASE ${tempDbName};`);
            console.log(`Temporary database '${tempDbName}' created.`);
        } catch (error) {
            console.error("Could not create temp db:", error);
        }

        const tempClient = new Pool({
            ...baseConfig,
            database: tempDbName,
        });

        try {
            const tempDbClient = await tempClient.connect();

            // Disable new connections to the original database
            await tempDbClient.query(`ALTER DATABASE ${currentDb} WITH ALLOW_CONNECTIONS false;`);

            // Terminate all connections except the current one
            await tempDbClient.query(`
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                AND datname = '${currentDb}';
            `);

            // Wait for the specified downtime
            await sleepms(downtime);

            // Re-enable new connections to the original database
            await tempDbClient.query(`ALTER DATABASE ${currentDb} WITH ALLOW_CONNECTIONS true;`);
            tempDbClient.release();
        } catch (error) {
            console.error(`Could not disable db ${currentDb}:`, error);
        } finally {
            await tempClient.end();
        }

        // Clean up the temporary database
        try {
            await mainClient.query(`
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                AND datname = '${tempDbName}';
            `);
            await mainClient.query(`DROP DATABASE ${tempDbName};`);
            console.log(`Temporary database '${tempDbName}' dropped.`);
        } catch (error) {
            console.error(`Could not clean up temp db ${tempDbName}:`, error);
        }
    } finally {
        mainClient.release();
    }
}

class DBOSSchedDBDown {
    static nCalls = 0;

    @Transaction({isolationLevel: "READ COMMITTED"})
    static async scheduledTxn(ctxt: TestTransactionContext) {
        await ctxt.client.query("SELECT 1");
        DBOSSchedDBDown.nCalls++;
    }

    @Scheduled({crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive})
    @Workflow()
    static async scheduledDefault(ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
        await ctxt.invoke(DBOSSchedDBDown).scheduledTxn();
        return Promise.resolve();
    }
}

describe("scheduled-wf-tests-when-db-down", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });
  
    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime([DBOSSchedDBDown], config);
        DBOSSchedDBDown.nCalls = 0;
    });
  
    afterEach(async () => {
        await testRuntime.destroy();
    }, 10000);

    test("test_appdb_downtime", async () => {
        // Make sure two functions with the same name in different classes are not interfering with each other.
        await sleepms(2000);
        const appdbname = await testRuntime.queryUserDB<{current_database: string}>("SELECT current_database()");
        const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
        const sysdb = dbosExec.systemDatabase as PostgresSystemDatabase;
        await simulateDbRestart(appdbname[0].current_database, 2000, sysdb.systemPoolConfig, sysdb.pool);
        await sleepms(2000);
        expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(3);
    });

    test("test_sysdb_downtime", async () => {
        await sleepms(2000);
        const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
        const sysdb = dbosExec.systemDatabase as PostgresSystemDatabase;
        const sysdbname = await sysdb.pool.query<{current_database: string}>("SELECT current_database()");
        await simulateDbRestart(sysdbname.rows[0].current_database, 2000, sysdb.systemPoolConfig, sysdb.pool);
        await sleepms(2000);
        expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(3);
    });
});

/*
def test_appdb_downtime(dbos: DBOS) -> None:
    wf_counter: int = 0

    time.sleep(2)
    simulate_db_restart(dbos._app_db.engine, 2)
    time.sleep(2)
    assert wf_counter > 2


def test_sysdb_downtime(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    time.sleep(2)
    simulate_db_restart(dbos._sys_db.engine, 2)
    time.sleep(2)
    # We know there should be at least 3 occurrences from the 4 seconds when the DB was up.
    #  There could be more than 4, depending on the pace the machine...
    assert wf_counter > 2
*/