import type { DBMigration } from '../migration_runner';

export function allMigrations(
  schemaName: string = 'dbos',
  opts?: { useListenNotify?: boolean },
): ReadonlyArray<DBMigration> {
  const useListenNotify = opts?.useListenNotify ?? true;
  return [
    {
      name: '20240123182943_schema',
      pg: [`CREATE SCHEMA IF NOT EXISTS "${schemaName}"`],
    },
    {
      name: '20240123182944_dbos_migrations',
      pg: [
        `create table "${schemaName}"."dbos_migrations" ("version" bigint not null, constraint "dbos_migrations_pkey" primary key ("version"))`,
      ],
    },
    {
      name: '20240123183021_tables',
      pg: [
        `create table "${schemaName}"."operation_outputs" ("workflow_uuid" text not null, "function_id" int4 not null, "output" text, "error" text, constraint "operation_outputs_pkey" primary key ("workflow_uuid", "function_id"))`,
        `create table "${schemaName}"."workflow_inputs" ("workflow_uuid" text not null, "inputs" text not null, constraint "workflow_inputs_pkey" primary key ("workflow_uuid"))`,
        `create table "${schemaName}"."workflow_status" ("workflow_uuid" text, "status" text, "name" text, "authenticated_user" text, "assumed_role" text, "authenticated_roles" text, "request" text, "output" text, "error" text, "executor_id" text, constraint "workflow_status_pkey" primary key ("workflow_uuid"))`,
        `create table "${schemaName}"."notifications" ("destination_uuid" text not null, "topic" text, "message" text not null, "created_at_epoch_ms" bigint not null default (EXTRACT(EPOCH FROM now())*1000)::bigint)`,
        `create table "${schemaName}"."workflow_events" ("workflow_uuid" text not null, "key" text not null, "value" text not null, constraint "workflow_events_pkey" primary key ("workflow_uuid", "key"))`,
      ],
    },
    {
      name: '20240123183025_indexes',
      pg: [`create index "idx_workflow_topic" on "${schemaName}"."notifications" ("destination_uuid", "topic")`],
    },
    {
      name: '20240123183030_triggers',
      pg: useListenNotify
        ? [
            `
    CREATE OR REPLACE FUNCTION "${schemaName}".notifications_function() RETURNS TRIGGER AS $$
    DECLARE
        payload text := NEW.destination_uuid || '::' || NEW.topic;
    BEGIN
        PERFORM pg_notify('dbos_notifications_channel', payload);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER dbos_notifications_trigger
    AFTER INSERT ON "${schemaName}".notifications
    FOR EACH ROW EXECUTE FUNCTION "${schemaName}".notifications_function();

    CREATE OR REPLACE FUNCTION "${schemaName}".workflow_events_function() RETURNS TRIGGER AS $$
    DECLARE
        payload text := NEW.workflow_uuid || '::' || NEW.key;
    BEGIN
        PERFORM pg_notify('dbos_workflow_events_channel', payload);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER dbos_workflow_events_trigger
    AFTER INSERT ON "${schemaName}".workflow_events
    FOR EACH ROW EXECUTE FUNCTION "${schemaName}".workflow_events_function();
  `,
          ]
        : [],
    },
    {
      name: '20240124015239_status_timestamp',
      pg: [
        `alter table "${schemaName}"."workflow_status" add column "created_at" bigint not null default (EXTRACT(EPOCH FROM now())*1000)::bigint, add column "updated_at" bigint not null default (EXTRACT(EPOCH FROM now())*1000)::bigint`,
      ],
    },
    {
      name: '20240201213211_replica_identity',
      pg: [
        `create extension if not exists "uuid-ossp"`,
        `alter table "${schemaName}"."notifications" add column "message_uuid" text not null default uuid_generate_v4()`,
        `alter table "${schemaName}"."notifications" add constraint "notifications_pkey" primary key ("message_uuid")`,
      ],
    },
    {
      name: '20240205223925_foreign_keys',
      pg: [
        `create index "workflow_status_created_at_index" on "${schemaName}"."workflow_status" ("created_at")`,
        `alter table "${schemaName}"."operation_outputs" add constraint "operation_outputs_workflow_uuid_foreign" foreign key ("workflow_uuid") references "${schemaName}"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
        `alter table "${schemaName}"."workflow_inputs" add constraint "workflow_inputs_workflow_uuid_foreign" foreign key ("workflow_uuid") references "${schemaName}"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
        `alter table "${schemaName}"."notifications" add constraint "notifications_destination_uuid_foreign" foreign key ("destination_uuid") references "${schemaName}"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
        `alter table "${schemaName}"."workflow_events" add constraint "workflow_events_workflow_uuid_foreign" foreign key ("workflow_uuid") references "${schemaName}"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
      ],
    },
    {
      name: '20240207192338_executor_id_index',
      pg: [`create index "workflow_status_executor_id_index" on "${schemaName}"."workflow_status" ("executor_id")`],
    },
    {
      name: '20240430090000_tables',
      pg: [
        `create table "${schemaName}"."scheduler_state" ("workflow_fn_name" text not null, "last_run_time" bigint not null, constraint "scheduler_state_pkey" primary key ("workflow_fn_name"))`,
      ],
    },
    {
      name: '20240516004341_application_version',
      pg: [
        `alter table "${schemaName}"."workflow_status" add column "application_version" text, add column "application_id" text`,
      ],
    },
    {
      name: '20240517000000_status_class_config',
      pg: [
        `alter table "${schemaName}"."workflow_status" add column "class_name" varchar(255) default null, add column "config_name" varchar(255) default null`,
      ],
    },
    {
      name: '20240621000000_workflow_tries',
      pg: [`alter table "${schemaName}"."workflow_status" add column "recovery_attempts" bigint default '0'`],
    },
    {
      name: '20240924000000_workflowqueue',
      pg: [
        `alter table "${schemaName}"."workflow_status" add column "queue_name" text default null`,
        `create table "${schemaName}"."workflow_queue" ("queue_name" text not null, "workflow_uuid" text not null, "created_at_epoch_ms" bigint not null default (EXTRACT(EPOCH FROM now())*1000)::bigint, "started_at_epoch_ms" bigint, "completed_at_epoch_ms" bigint, constraint "workflow_queue_pkey" primary key ("workflow_uuid"))`,
        `alter table "${schemaName}"."workflow_queue" add constraint "workflow_queue_workflow_uuid_foreign" foreign key ("workflow_uuid") references "${schemaName}"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
      ],
    },
    {
      name: '20241009150000_event_dispatch_kv',
      pg: [
        `create table "${schemaName}"."event_dispatch_kv" ("service_name" text not null, "workflow_fn_name" text not null, "key" text not null, "value" text, "update_seq" decimal(38, 0), "update_time" decimal(38, 15), constraint "event_dispatch_kv_pkey" primary key ("service_name", "workflow_fn_name", "key"))`,
      ],
    },
    {
      name: '20250312171547_function_name_op_outputs',
      pg: [`alter table "${schemaName}"."operation_outputs" add column "function_name" text not null default ''`],
    },
    {
      name: '20250319190617_add_childid_opoutputs',
      pg: [`alter table "${schemaName}"."operation_outputs" add column "child_workflow_id" text`],
    },
    {
      name: '20252101000000_workflow_queues_executor_id',
      pg: [`alter table "${schemaName}"."workflow_queue" add column "executor_id" text`],
    },
    {
      name: '20252501190959_queue_dedup_id',
      pg: [
        `alter table "${schemaName}"."workflow_queue" add column "deduplication_id" text`,
        `alter table "${schemaName}"."workflow_queue" add constraint "workflow_queue_queue_name_deduplication_id_unique" unique ("queue_name", "deduplication_id")`,
      ],
    },
    {
      name: '20252505000000_workflow_timeout',
      pg: [
        `alter table "${schemaName}"."workflow_status" add column "workflow_timeout_ms" bigint, add column "workflow_deadline_epoch_ms" bigint`,
      ],
    },
    {
      name: '20252512000000_queue_priority',
      pg: [`alter table "${schemaName}"."workflow_queue" add column "priority" int4 not null default '0'`],
    },
    {
      name: '20252523000000_consolidate_inputs',
      pg: [`alter table "${schemaName}"."workflow_status" add column "inputs" text null`],
    },
    {
      name: '20252528000000_consolidate_queues',
      pg: [
        `alter table "${schemaName}"."workflow_status" add column "started_at_epoch_ms" bigint null, add column "deduplication_id" text null, add column "priority" int4 not null default '0'`,
        `alter table "${schemaName}"."workflow_status" add constraint "uq_workflow_status_queue_name_dedup_id" unique ("queue_name", "deduplication_id")`,
        `create index "workflow_status_status_index" on "${schemaName}"."workflow_status" ("status")`,
      ],
    },
    {
      name: '20252806000000_streaming',
      pg: [
        `create table "${schemaName}"."streams" ("workflow_uuid" text not null, "key" text not null, "value" text not null, "offset" int4 not null, constraint "streams_pkey" primary key ("workflow_uuid", "key", "offset"))`,
        `alter table "${schemaName}"."streams" add constraint "streams_workflow_uuid_foreign" foreign key ("workflow_uuid") references "${schemaName}"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
      ],
    },
    {
      name: '20252810000000_partitioned_queues',
      pg: [`alter table "${schemaName}"."workflow_status" add column "queue_partition_key" text`],
    },
    /**
     * Supports the rate-limit COUNT at src/system_database.ts:2078 by aligning the
     * query with a queue_name/status/started_at index so the planner can choose it
     * instead of falling back to the uq_workflow_status_queue_name_dedup_id unique
     * index, which forces a scan across all statuses.
     */
    {
      name: '20252950000000_workflow_status_queue_status_started_index',
      pg: [
        `create index "idx_workflow_status_queue_status_started" on "${schemaName}"."workflow_status" ("queue_name", "status", "started_at_epoch_ms")`,
      ],
    },
    {
      pg: [
        `ALTER TABLE "${schemaName}".workflow_status ADD COLUMN forked_from TEXT;`,
        `create index "idx_workflow_status_forked_from" on "${schemaName}"."workflow_status" ("forked_from")`,
      ],
    },
    {
      pg: [
        `ALTER TABLE "${schemaName}".operation_outputs ADD COLUMN started_at_epoch_ms BIGINT, ADD COLUMN completed_at_epoch_ms BIGINT;`,
      ],
    },
    {
      pg: [`ALTER TABLE "${schemaName}"."workflow_status" ADD COLUMN "owner_xid" VARCHAR(40) DEFAULT NULL`],
    },
    {
      pg: [
        `
        CREATE TABLE "${schemaName}".workflow_events_history (
            workflow_uuid TEXT NOT NULL,
            function_id INT4 NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (workflow_uuid, function_id, key),
            FOREIGN KEY (workflow_uuid) REFERENCES "${schemaName}".workflow_status(workflow_uuid)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        `,
        `ALTER TABLE "${schemaName}".streams ADD COLUMN function_id INT4 NOT NULL DEFAULT 0;`,
      ],
    },
    {
      pg: [
        `ALTER TABLE "${schemaName}"."workflow_status" ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;`,
        `CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "${schemaName}"."workflow_status" ("parent_workflow_id");`,
      ],
    },
    {
      pg: [
        `ALTER TABLE "${schemaName}"."workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL`,
        `ALTER TABLE "${schemaName}"."notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL`,
        `ALTER TABLE "${schemaName}"."workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL`,
        `ALTER TABLE "${schemaName}"."workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL`,
        `ALTER TABLE "${schemaName}"."operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL`,
        `ALTER TABLE "${schemaName}"."streams" ADD COLUMN "serialization" TEXT DEFAULT NULL`,
      ],
    },
    {
      pg: [
        `CREATE TABLE "${schemaName}"."workflow_schedules" (
          schedule_id TEXT PRIMARY KEY,
          schedule_name TEXT NOT NULL UNIQUE,
          workflow_name TEXT NOT NULL,
          workflow_class_name TEXT,
          schedule TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'ACTIVE',
          context TEXT NOT NULL
        )`,
      ],
    },
  ];
}
