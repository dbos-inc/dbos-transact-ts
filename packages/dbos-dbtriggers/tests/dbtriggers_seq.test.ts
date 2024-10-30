import { Knex } from "knex";

import {
    createTestingRuntime,
    TestingRuntime,
    Transaction,
    TransactionContext,
    Workflow,
    WorkflowContext
} from "@dbos-inc/dbos-sdk";

import { DBTriggerWorkflow, TriggerOperation } from "../dbtrigger/dbtrigger";

function sleepms(ms: number) {return new Promise((r) => setTimeout(r, ms)); }

const testTableName = "dbos_test_trig_seq";

type KnexTransactionContext = TransactionContext<Knex>;

class DBOSTestNoClass {

}

class DBOSTriggerTestClassSN {
    static nTSUpdates = 0;
    static tsRecordMap: Map<number, TestTable> = new Map();

    static nSNUpdates = 0;
    static snRecordMap: Map<number, TestTable> = new Map();

    static reset() {
        DBOSTriggerTestClassSN.nTSUpdates = 0;
        DBOSTriggerTestClassSN.tsRecordMap = new Map();

        DBOSTriggerTestClassSN.nSNUpdates = 0;
        DBOSTriggerTestClassSN.snRecordMap = new Map();
    }

    @DBTriggerWorkflow({tableName: testTableName, recordIDColumns: ['order_id'], sequenceNumColumn: 'seqnum', sequenceNumJitter: 2, installDBTrigger: true})
    @Workflow()
    static async triggerWFBySeq(ctxt: WorkflowContext, op: TriggerOperation, key: number[], rec: unknown) {
        ctxt.logger.debug(`WFSN ${op} - ${JSON.stringify(key)} / ${JSON.stringify(rec)}`);
        expect (op).toBe(TriggerOperation.RecordUpserted);
        if (op === TriggerOperation.RecordUpserted) {
            DBOSTriggerTestClassSN.snRecordMap.set(key[0], rec as TestTable);
            ++DBOSTriggerTestClassSN.nSNUpdates;
        }
        return Promise.resolve();
    }

    @DBTriggerWorkflow({tableName: testTableName, recordIDColumns: ['order_id'], timestampColumn: 'update_date', timestampSkewMS: 60000, installDBTrigger: true})
    @Workflow()
    static async triggerWFByTS(ctxt: WorkflowContext, op: TriggerOperation, key: number[], rec: unknown) {
        ctxt.logger.debug(`WFTS ${op} - ${JSON.stringify(key)} / ${JSON.stringify(rec)}`);
        expect (op).toBe(TriggerOperation.RecordUpserted);
        if (op === TriggerOperation.RecordUpserted) {
            DBOSTriggerTestClassSN.tsRecordMap.set(key[0], rec as TestTable);
            ++DBOSTriggerTestClassSN.nTSUpdates;
        }
        return Promise.resolve();
    }

    @Transaction()
    static async insertRecord(ctx: KnexTransactionContext, rec: TestTable) {
        await ctx.client<TestTable>(testTableName).insert(rec);
    }

    @Transaction()
    static async deleteRecord(ctx: KnexTransactionContext, order_id: number) {
        await ctx.client<TestTable>(testTableName).where({order_id}).delete();
    }

    @Transaction()
    static async updateRecordStatus(ctx: KnexTransactionContext, order_id: number, status: string, seqnum: number, update_date: Date) {
        await ctx.client<TestTable>(testTableName).where({order_id}).update({status, seqnum, update_date});
    }
}

interface TestTable {
    order_id: number,
    seqnum: number,
    update_date: Date,
    price: number,
    item: string,
    status: string,
}

describe("test-db-triggers", () => {
    let testRuntime: TestingRuntime;

    beforeAll(async () => {
    });

    beforeEach(async () => {
        testRuntime = await createTestingRuntime([DBOSTestNoClass], 'dbtriggers-test-dbos-config.yaml');
        await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
        await testRuntime.queryUserDB(`
            CREATE TABLE IF NOT EXISTS ${testTableName}(
              order_id SERIAL PRIMARY KEY,
              seqnum INTEGER,
              update_date TIMESTAMP,
              price DECIMAL(10,2),
              item TEXT,
              status VARCHAR(10)
            );`
        );
        await testRuntime.destroy();
        testRuntime = await createTestingRuntime(undefined, 'dbtriggers-test-dbos-config.yaml');
        DBOSTriggerTestClassSN.reset()
    });

    afterEach(async () => {
        await testRuntime.deactivateEventReceivers();
        await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
        await testRuntime.destroy();
    });

    test("trigger-seqnum", async () => {
        await testRuntime.invoke(DBOSTriggerTestClassSN).insertRecord({order_id: 1, seqnum: 1, update_date: new Date('2024-01-01 11:11:11'), price: 10, item: "Spacely Sprocket", status:"Ordered"});
        await testRuntime.invoke(DBOSTriggerTestClassSN).updateRecordStatus(1, "Packed", 2, new Date('2024-01-01 11:11:12'));
        while (DBOSTriggerTestClassSN.nSNUpdates < 2 || DBOSTriggerTestClassSN.nTSUpdates < 2) await sleepms(10);
        expect(DBOSTriggerTestClassSN.nSNUpdates).toBe(2);
        expect(DBOSTriggerTestClassSN.nTSUpdates).toBe(2);
        expect(DBOSTriggerTestClassSN.snRecordMap.get(1)?.status).toBe("Packed");
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status).toBe("Packed");

        await testRuntime.invoke(DBOSTriggerTestClassSN).insertRecord({order_id: 2, seqnum: 3, update_date: new Date('2024-01-01 11:11:13'), price: 10, item: "Cogswell Cog", status:"Ordered"});
        await testRuntime.invoke(DBOSTriggerTestClassSN).updateRecordStatus(1, "Shipped", 5, new Date('2024-01-01 11:11:15'));
        while (DBOSTriggerTestClassSN.nSNUpdates < 4 || DBOSTriggerTestClassSN.nTSUpdates < 4) await sleepms(10);
        expect(DBOSTriggerTestClassSN.nSNUpdates).toBe(4);
        expect(DBOSTriggerTestClassSN.nTSUpdates).toBe(4);
        expect(DBOSTriggerTestClassSN.snRecordMap.get(1)?.status).toBe("Shipped");
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status).toBe("Shipped");
        expect(DBOSTriggerTestClassSN.snRecordMap.get(2)?.status).toBe("Ordered");
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(2)?.status).toBe("Ordered");

        // Take down
        await testRuntime.deactivateEventReceivers();

        // Do more stuff
        // Invalid record, won't show up because it is well out of sequence
        await testRuntime.invoke(DBOSTriggerTestClassSN).insertRecord({order_id: 999, seqnum: -999, update_date: new Date('1900-01-01 11:11:13'), price: 10, item: "Cogswell Cog", status:"Ordered"});

        // A few more valid records, back in time a little
        await testRuntime.invoke(DBOSTriggerTestClassSN).insertRecord({order_id: 3, seqnum: 4, update_date: new Date('2024-01-01 11:11:14'), price: 10, item: "Griswold Gear", status:"Ordered"});
        await testRuntime.invoke(DBOSTriggerTestClassSN).insertRecord({order_id: 4, seqnum: 6, update_date: new Date('2024-01-01 11:11:16'), price: 10, item: "Wallace Wheel", status:"Ordered"});
        await testRuntime.invoke(DBOSTriggerTestClassSN).updateRecordStatus(4, "Shipped", 7, new Date('2024-01-01 11:11:17'));

        // Test restore
        console.log("************************************************** Restart *****************************************************");
        DBOSTriggerTestClassSN.reset();

        await testRuntime.initEventReceivers();

        console.log("************************************************** Restarted *****************************************************");
        DBOSTriggerTestClassSN.reset();
        // We had processed up to 5 before,
        // The count of 7 is a bit confusing because we're sharing a table.  We expect all 4 orders to be sent based on time, and 3 based on SN
        while (DBOSTriggerTestClassSN.nSNUpdates < 7 || DBOSTriggerTestClassSN.nTSUpdates < 7) await sleepms(10);
        await sleepms(100);

        console.log("************************************************** Catchup Complete *****************************************************");

        expect(DBOSTriggerTestClassSN.nSNUpdates).toBe(7);
        // With 60 seconds, we will see all records again
        expect(DBOSTriggerTestClassSN.nTSUpdates).toBe(7);

        expect(DBOSTriggerTestClassSN.snRecordMap.get(1)?.status).toBe("Shipped");
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status).toBe("Shipped");
        expect(DBOSTriggerTestClassSN.snRecordMap.get(2)?.status).toBe("Ordered");
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(2)?.status).toBe("Ordered");
        expect(DBOSTriggerTestClassSN.snRecordMap.get(3)?.status).toBe("Ordered");
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(3)?.status).toBe("Ordered");
        expect(DBOSTriggerTestClassSN.snRecordMap.get(4)?.status).toBe("Shipped");
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(4)?.status).toBe("Shipped");
        expect(DBOSTriggerTestClassSN.snRecordMap.get(999)?.status).toBeUndefined();
        expect(DBOSTriggerTestClassSN.tsRecordMap.get(999)?.status).toBeUndefined();
    }, 15000);
});

