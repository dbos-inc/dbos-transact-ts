# Responding To Database Updates: DBOS Postgres Notification Trigger Library

## Scenarios

In some cases, DBOS Transact workflows should be triggered in response to database inserts or updates.

For example:

- Insertion of new orders into the orders table should cause a fulfillment workflow to start
- Update of an order from "ordered" to "shipped" should schedule a review workflow later
- Exception events logged to an event table should be transformed and inserted into a reporting table

Of course, if the process that is performing the database insert / update is running within DBOS Transact, the best solution would be to have that workflow start additional workflows directly. However, in some cases, the broader system is not written with DBOS Transact, and there are no notifications to receive, leaving "database snooping" as the best option.

## Is This a Library?

Yes, in circumstances where the database is Postgres, this package can be used directly as a library to listen for database record updates, and initiate workflows. However, situations vary widely, so it is just as likely that the code in this package would be used as a reference for a custom database listener implementation.

There are many considerations that factor in to the design of database triggers, including:

- What database product is in use? This library uses drivers and SQL code specific to Postgres. Some adjustments may be necessary to support other databases and clients.
- Are database triggers and notifications possible? While some databases will run a trigger function upon record insert/update, and provide a mechanism to notify database clients, some databases do not support this. In some environments, the database administrator or policies may not permit the installation of triggers or the use of the notifications feature, and polling should be used.
- How is new data identified? This library supports sequence number and timestamp fields within the records as a mechanism for identifying recent records. If these techniques are not sufficient, customization will be required.

## General Techniques For Detecting Database Updates

There are three broad strategies for detecting or identifying new and updated database records.

1. Many databases have a [log](https://en.wikipedia.org/wiki/Write-ahead_logging) that can be used to find record updates. This package does not use the database log. If you would like to use a strategy based on the database log, it may be best to use a third-party [CDC](https://en.wikipedia.org/wiki/Change_data_capture) tool such as [Debezium](https://debezium.io/) that streams the log as events. DBOS apps can then subscribe to the events.
2. Some databases support stored procedures and triggers, such that when a table is updated, execution of a stored procedure is triggered. The stored procedure can then notify clients of the database changes. Note that this mechanism, while useful for detecting changes quickly, is generally not sufficiently reliable by itself, as any changes that occur when no client is connected may go unnoticed.
3. Queries are a generic strategy for finding new records, as long as a suitable query predicate can be formulated. When the query is run in a polling loop, the results are likely new records. After the batch of new records found in a loop iteration is processed, the query predicate is adjusted forward to avoid reading records that were already processed. While use of polling loops can involve longer processing delays and does not "scale to zero" like triggers, queries require only read access to the database table and are therefore usable in a wider range of scenarios.

This library supports polling via queries, and also supports triggers and notifications. Use of triggers and notifications also requires queries, as the queries are used on startup to identify any "backfill" / "make-up work" due to records that were updated when the client was not listening for notifications.

## Using This Package

This package provides method decorators that, when applied to DBOS functions, will listen or poll for database changes and invoke the decorated function as changes occur.

### Creating and Configuring a Listener

First, create an instance of `DBTrigger`. As the `DBTrigger` instance needs connections to a Postgres database and run queries, functions should be provided. For example:

```typescript
// Get database configuration from environment (your approach may vary)
const config = {
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432'),
  database: process.env.PGDATABASE || 'postgres',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || 'dbos',
};

const pool = new Pool(config);

// Creation of DBTrigger listener, used for decorations below
const trig = new DBTrigger({
  // Called to get a long-lived connection for notification listener
  connect: async () => {
    const conn = pool.connect();
    return conn;
  },
  // Return listener connection (for example, shutdown)
  disconnect: async (c: ClientBase) => {
    (c as PoolClient).release();
    return Promise.resolve();
  },
  // Execute a query (polling; optionally, trigger installation)
  query: async <R>(sql: string, params?: unknown[]) => {
    return (await pool.query(sql, params)).rows as R[];
  },
});

// Use the trigger object to decorate static async class methods
class TriggeredFunctions {
  @trig.triggerWorkflow({ tableName: testTableName, recordIDColumns: ['order_id'], installDBTrigger: true })
  @DBOS.workflow()
  static async triggerWF(op: TriggerOperation, key: number[], rec: unknown) {
    ...
  }
}
```

### Decorating Functions

This database trigger package supports workflow and non-workflow functions. Workflow functions are to be used in cases where records should be processed once. With workflow functions, a query is required, and database notifications are optional. With non-workflow functions, triggers are the only supported method for detecting database changes.

#### Decorating Workflow Methods

Workflow methods marked with `<trigger>.@triggerWorkflow` will run in response to database records. Workflows are guaranteed to run exactly once per record in the source database, provided that new records can be identified by querying the source table using simple predicates. The workflow method must:

- Be `async`, `static`, and decorated with `@DBOS.workflow`
- Have the arguments `op: TriggerOperation, key: unknown[], rec: unknown`

The decorator is `DBTriggerWorkflow(triggerConfig: DBTriggerConfig)`. The parameters provided to each method invocation are:

- `op`: The operation (insert/update/delete) that occurred.
- `key`: An array of record fields that have been extracted as the record key. The list of fields extracted is controlled by the `DBTriggerConfig`.
- `rec`: The new contents of the database record.

`op` is taken from the `TriggerOperation` enum:

```typescript
export enum TriggerOperation {
  RecordInserted = 'insert',
  RecordDeleted = 'delete',
  RecordUpdated = 'update',
  RecordUpserted = 'upsert',
}
```

Note that while the database trigger and notification can detect deletes and tell the difference between a record insert and update, database polling cannot. Records detected by polling will therefore always be reported as `upsert`, and could have been an `insert` or `update`.

If you need full historical details, an append-only table should be used. This table can be maintained using a trigger from the base table. Alternatively, a CDC tool could be used.

#### Decorating Plain Methods

Non-workflow methods decorated with `@<trigger>.trigger` will also be run in response to database events. Note that, in contrast to workflows, this approach does not provide the guarantee of exactly-once method execution. The class method decorated must:

- Be `async` and `static`
- Have the arguments `op: TriggerOperation, key: unknown[], rec: unknown`, which will be provided when the trigger invokes the method.

The decorator is `trigger(triggerConfig: DBTriggerConfig)`. The parameters provided to each method invocation are:

- `op`: The operation (insert/update/delete) that occurred. See [Decorating Workflow Methods](#decorating-workflow-methods) above for more details.
- `key`: An array of record fields that have been extracted as the record key. The list of fields extracted is controlled by the `DBTriggerConfig`.
- `rec`: The new contents of the database record.

### Decorator Configuration

To detect changes in the source database, several configuration items are needed. This configuration is captured in the `DBTriggerConfig` interface:

```typescript
export class DBTriggerConfig {
  tableName: string = ''; // Database table to trigger
  schemaName?: string = undefined; // Database table schema (optional)

  // These identify the record, for elevation to function parameters
  recordIDColumns?: string[] = undefined;

  // Should DB trigger / notification be used?  Or just polling?
  useDBNotifications?: boolean = false;

  // Should DB trigger be auto-installed?  If not, a migration should install the trigger
  installDBTrigger?: boolean = false;

  // This identify the record sequence number, for checkpointing
  sequenceNumColumn?: string = undefined;
  // In case sequence numbers aren't perfectly in order, how far off could they be?
  sequenceNumJitter?: number = undefined;

  // This identifies the record timestamp, for checkpointing
  timestampColumn?: string = undefined;
  // In case sequence numbers aren't perfectly in order, how far off could they be?
  timestampSkewMS?: number = undefined;

  // Use a workflow queue if set
  queueName?: string = undefined;

  // If not using triggers, frequency of polling, ms
  dbPollingInterval?: number = 5000;
}
```

#### Source Table

The first key piece of configuration is the source table, identified by the `tableName` and optional `schemaName`. This table will be polled, or have a trigger installed.

`recordIDColumns` may be specified. The columns listed will be elevated as arguments to the invoked function, but also, in conjunction with the record sequence number or timestamp, serve as the [idempotency key](https://docs.dbos.dev/typescript/tutorials/idempotency-tutorial) for workflows.

#### Using Database Triggers

If `useDBNotifications` or `installDBTrigger` is set, DBOS Transact will listen for database notifications, and use these to trigger the function. If `installDBTrigger` is set, an attempt will be made to install a stored procedure and trigger into the source database. If `installDBTrigger` is not set, code for this trigger procedure will be produced on application startup, and can be placed into the database migration scheme of choice.

#### Polling and Catchup Queries

In order to detect database changes when notifications are not available (either entirely, or intermittently), database queries are used. The `dbos-dbtriggers` package manages this process by selecting recent records, and then storing a checkpoint of the largest timestamp or sequence number processed. The checkpoint is stored in the DBOS system database.

If source records are inserted or updated in a roughly chronological order, `timestampColumn` should be set to the name of the column containing the timestamp. If a `timestampColumn` is provided, the value will be used as part of the workflow key, and checkpointed to the system database when records are processed. If timestamps may be out of order slightly, `timestampSkewMS` can be provided. The predicate used to query the source table for new records will be adjusted by this amount. Note that while this may cause some records to be reprocessed, workflow idempotency properties eliminate any correctness consequences.

Alternatively, if source records are inserted or updated with a sequence number, `sequenceNumColumn` should be set to the name of the column containing the sequence. If a `sequenceNumColumn` is provided, the value will be used as part of the workflow key, and checkpointed to the system database when records are processed. If records may be out of order slightly, `sequenceNumJitter` can be provided. The predicate used to query the source table for new records will be adjusted by this amount. Note that while this may cause some records to be reprocessed, workflow idempotency properties eliminate any correctness consequences.

The information above is always used by methods decorated with `@<trigger>.triggerWorkflow`, for the formulation of catch-up queries. If `useDBNotifications` and `installDBTrigger` are both false, the configuration will also be used to generate queries for polling the source table. A query will be scheduled every `dbPollingInterval` milliseconds.

#### Using Workflow Queues for Concurrency and Rate Limiting

By default, `@<trigger>.triggerWorkflow` workflows are started immediately upon receiving database updates. If `queueName` is provided to the `DBTriggerConfig`, then the workflows will be enqueued in a [workflow queue](https://docs.dbos.dev/typescript/reference/transactapi/workflow-queues) and subject to rate limits.

## Using This Code As A Starting Point

The `dbos-dbtriggers` package can be used as a starting point for a custom solution. It is loosely broken into the following parts:

- Decorators and configuration
- An [event receiver](https://docs.dbos.dev/typescript/tutorials/requestsandevents/custom-event-receiver), which handles the process of listening to the database and invoking workflows
- Tests, which perform database operations and ensure the trigger functions are executed under a variety of conditions, including system restarts.

## Next Steps

- To learn how to create an application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/quickstart)
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
