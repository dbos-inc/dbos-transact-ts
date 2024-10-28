# Responding To Database Updates: DBOS Database Trigger Library

## Scenarios

In some cases, DBOS Transact workflows should be triggered in response to database inserts or updates.

For example:
*  Insertion of new orders into the orders table should cause a fulfillment workflow to start
*  Update of an order from "ordered" to "shipped" should schedule a review workflow later
*  Exception events logged to an event table should be transformed and inserted into a reporting table

Of course, if the process that is performing the database insert / update is running within DBOS Transact, the best solution would be to have that workflow start additional workflows directly.  However, in some cases, the broader system is not written with DBOS Transact, and there are no notifications to receive, leaving "database snooping as" the best option.

## Is This a Library?

Yes, in many circumstances this package can be used directly as a library to listen for database record updates, and initiate workflows.  However, situations vary widely, so it is just as likely that the code in this package would be used as a reference for a custom database listener implementation.

There are many considerations that factor in to the design of database triggers, including:
*  Which database holds the table to be watched?  This library is specifically designed to read from tables in the application user database.  If the table is elsewhere, it would be necessary to extend the code.
*  What database product is in use?  This library uses drivers and SQL code specific to Postgres.  Some adjustments may be necessary to support other databases.
*  Are database triggers and notifications possible?  While some databases will run a trigger function upon record insert/update, and provide a mechanism to notify database clients, some databases do not support this.   In some environments, the database administrator or policies may not permit the installation of triggers or the use of the notifications feature.
*  How is new data identified?  This library supports sequence number and timestamp fields within the records as a mechanism for identifying recent records.  If these techniques are not sufficient, customization will be required.

## General Techniques For Detecting Database Updates
There are three broad strategies for detecting or identifying new and updated database records.
1. Many databases have a [log](https://en.wikipedia.org/wiki/Write-ahead_logging) that can be used to find record updates.  This package does not use the database log.  If you would like to use a strategy based on the database log, it may be best to use a third-party [CDC](https://en.wikipedia.org/wiki/Change_data_capture) tool such as [Debezium](https://debezium.io/) that streams the log as events.  DBOS apps can then subscribe to the events.
2. Some databases support stored procedures and triggers, such that when a table is updated, execution of a stored procedure is triggered.  The stored procedure can then notify clients of the database changes.  Note that this mechanism, while useful for detecting changes quickly, is generally not sufficiently reliable, as any changes that occur when no client is connected may go unnoticed.
3. Queries are a generic strategy for finding new records, as long as a suitable query predicate can be formulated.  When the query is run in a polling loop, the results are likely new records.  After the batch of new records found in a loop iteration is processed, the query predicate is adjusted forward to reflect that records were already processed.  While use of polling loops can involve longer processing delays and does not "scale to zero" like triggers, queries require only read access to the database table and are therefore usable in a wider range of scenarios.

This library supports polling via queries, and also supports triggers and notifications.  Use of triggers and notifications also requires queries, as the queries are used on startup to identify any "backfill" / "make-up work" due to records that were updated when the client was not listening for notifications.

## Using This Package
This package provides method decorators that, when applied to DBOS functions, will listen for database changes and invoke the decorated function as changes occur.

### Decorating Functions

#### Decorating Workflow Methods
Workflow methods can be run in response to database records.  Workflows are guaranteed to run exactly once per record in the source database, provided that new records can be identified by querying the source table using simple predicates.  The workflow method must:
* Be `async`, `static`, and decorated with `@Workflow`
* Have the arguments `ctxt: WorkflowContext, op: TriggerOperation, key: unknown[], rec: unknown`

The parameters provided to each method invocation are:
* `ctxt`: The [workflow context](https://docs.dbos.dev/typescript/reference/contexts#workflowcontext).
* `op`: The operation (insert/update/delete) that occurred.
* `key`: An array of record fields that have been extracted as the record key.  The list of fields extracted is controlled by the `DBTriggerConfig`.
* `rec`: The new contents of the database record.

#### Decorating Plain Methods
Non-workflow methods can also be run in response to database events.  Note that, in contrast to workflows, this approach does not provide the guarantee of exactly-once execution of the method.  The class method decorated must:
* Be `async` and `static`
* Have the arguments `op: TriggerOperation, key: unknown[], rec: unknown`, which will be provided when the trigger invokes the method.

The parameters provided to each method invocation are:
* `op`: The operation (insert/update/delete) that occurred.  See [Decorating Workflow Methods](#decorating-workflow-methods) above for more details.
* `key`: An array of record fields that have been extracted as the record key.  The list of fields extracted is controlled by the `DBTriggerConfig`.
* `rec`: The new contents of the database record.

### Using Polling

### Using Database Triggers

### Using Workflow Queues

#### Automatic Database Trigger Installation

#### Generating Database Trigger Migrations

## Using This Code As A Starting Point

## Next Steps
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
