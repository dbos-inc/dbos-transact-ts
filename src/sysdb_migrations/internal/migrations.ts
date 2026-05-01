import type { DBMigration } from '../migration_runner';

export function allMigrations(
  schemaName: string = 'dbos',
  opts?: { useListenNotify?: boolean; isCockroach?: boolean },
): ReadonlyArray<DBMigration> {
  const useListenNotify = opts?.useListenNotify ?? true;
  const isCockroach = opts?.isCockroach ?? false;
  const c = isCockroach ? '' : 'CONCURRENTLY';
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
    {
      pg: [
        `ALTER TABLE "${schemaName}"."notifications" ADD COLUMN "consumed" BOOLEAN NOT NULL DEFAULT false`,
        `CREATE INDEX "idx_notifications" ON "${schemaName}"."notifications" ("destination_uuid", "topic")`,
      ],
    },
    {
      pg: [
        `CREATE TABLE "${schemaName}"."application_versions" (
          "version_id" TEXT NOT NULL,
          "version_name" TEXT NOT NULL UNIQUE,
          "version_timestamp" BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000.0)::bigint,
          "created_at" BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000.0)::bigint,
          CONSTRAINT "application_versions_pkey" PRIMARY KEY ("version_id")
        )`,
      ],
    },
    {
      pg: [
        `CREATE FUNCTION "${schemaName}".enqueue_workflow(
            workflow_name TEXT,
            queue_name TEXT,
            positional_args JSON[] DEFAULT ARRAY[]::JSON[],
            named_args JSON DEFAULT '{}'::JSON,
            class_name TEXT DEFAULT NULL,
            config_name TEXT DEFAULT NULL,
            workflow_id TEXT DEFAULT NULL,
            app_version TEXT DEFAULT NULL,
            timeout_ms BIGINT DEFAULT NULL,
            deadline_epoch_ms BIGINT DEFAULT NULL,
            deduplication_id TEXT DEFAULT NULL,
            priority INTEGER DEFAULT NULL,
            queue_partition_key TEXT DEFAULT NULL
        ) RETURNS TEXT AS $$
        DECLARE
            v_workflow_id TEXT;
            v_serialized_inputs TEXT;
            v_owner_xid TEXT;
            v_now BIGINT;
            v_recovery_attempts INTEGER := 0;
            v_priority INTEGER;
        BEGIN

            -- Validate required parameters
            IF workflow_name IS NULL OR workflow_name = '' THEN
                RAISE EXCEPTION 'Workflow name cannot be null or empty';
            END IF;
            IF queue_name IS NULL OR queue_name = '' THEN
                RAISE EXCEPTION 'Queue name cannot be null or empty';
            END IF;
            IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
                RAISE EXCEPTION 'Named args must be a JSON object';
            END IF;
            IF workflow_id IS NOT NULL AND workflow_id = '' THEN
                RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
            END IF;

            v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
            v_owner_xid := gen_random_uuid()::TEXT;
            v_priority := COALESCE(priority, 0);
            v_serialized_inputs := json_build_object(
                'positionalArgs', positional_args,
                'namedArgs', named_args
            )::TEXT;
            v_now := EXTRACT(epoch FROM now()) * 1000;

            INSERT INTO "${schemaName}".workflow_status (
                workflow_uuid, status, inputs,
                name, class_name, config_name,
                queue_name, deduplication_id, priority, queue_partition_key,
                application_version,
                created_at, updated_at, recovery_attempts,
                workflow_timeout_ms, workflow_deadline_epoch_ms,
                parent_workflow_id, owner_xid, serialization
            ) VALUES (
                v_workflow_id, 'ENQUEUED', v_serialized_inputs,
                workflow_name, class_name, config_name,
                queue_name, deduplication_id, v_priority, queue_partition_key,
                app_version,
                v_now, v_now, v_recovery_attempts,
                timeout_ms, deadline_epoch_ms,
                NULL, v_owner_xid, 'portable_json'
            )
            ON CONFLICT (workflow_uuid)
            DO UPDATE SET
                updated_at = EXCLUDED.updated_at;

            RETURN v_workflow_id;

        EXCEPTION
            WHEN unique_violation THEN
                RAISE EXCEPTION 'DBOS queue duplicated'
                   USING DETAIL = format('Workflow %s with queue %s and deduplication ID %s already exists', v_workflow_id, queue_name, deduplication_id),
                        ERRCODE = 'unique_violation';
        END;
        $$ LANGUAGE plpgsql;`,
        `CREATE FUNCTION "${schemaName}".send_message(
            destination_id TEXT,
            message JSON,
            topic TEXT DEFAULT NULL,
            message_id TEXT DEFAULT NULL
        ) RETURNS VOID AS $$
        DECLARE
            v_topic TEXT := COALESCE(topic, '__null__topic__');
            v_message_id TEXT := COALESCE(message_id, gen_random_uuid()::TEXT);
        BEGIN
            INSERT INTO "${schemaName}".notifications (
                destination_uuid, topic, message, message_uuid, serialization
            ) VALUES (
                destination_id, v_topic, message, v_message_id, 'portable_json'
            )
            ON CONFLICT (message_uuid) DO NOTHING;
        EXCEPTION
            WHEN foreign_key_violation THEN
                RAISE EXCEPTION 'DBOS non-existent workflow'
                   USING DETAIL = format('Destination workflow %s does not exist', destination_id),
                        ERRCODE = 'foreign_key_violation';
        END;
        $$ LANGUAGE plpgsql;`,
      ],
    },
    {
      pg: [
        `ALTER TABLE "${schemaName}"."workflow_schedules" ADD COLUMN "last_fired_at" TEXT DEFAULT NULL`,
        `ALTER TABLE "${schemaName}"."workflow_schedules" ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE`,
        `ALTER TABLE "${schemaName}"."workflow_schedules" ADD COLUMN "cron_timezone" TEXT DEFAULT NULL`,
      ],
    },
    {
      pg: [
        `ALTER TABLE "${schemaName}"."workflow_status" ADD COLUMN "delay_until_epoch_ms" BIGINT DEFAULT NULL`,
        `CREATE INDEX "idx_workflow_status_delayed" ON "${schemaName}"."workflow_status" ("delay_until_epoch_ms") WHERE status = 'DELAYED'`,
      ],
    },
    {
      pg: [`ALTER TABLE "${schemaName}"."workflow_schedules" ADD COLUMN "queue_name" TEXT DEFAULT NULL`],
    },
    {
      pg: [`ALTER TABLE "${schemaName}"."workflow_status" ADD COLUMN "was_forked_from" BOOLEAN NOT NULL DEFAULT FALSE`],
    },
    {
      pg: [
        `CREATE INDEX "idx_operation_outputs_completed_at_function_name" ON "${schemaName}"."operation_outputs" ("completed_at_epoch_ms", "function_name")`,
      ],
    },
    {
      pg: isCockroach
        ? []
        : [
            `ALTER FUNCTION "${schemaName}".enqueue_workflow(
            TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INTEGER, TEXT
        ) SET search_path = pg_catalog, pg_temp;`,
            `ALTER FUNCTION "${schemaName}".send_message(
            TEXT, JSON, TEXT, TEXT
        ) SET search_path = pg_catalog, pg_temp;`,
            ...(useListenNotify
              ? [
                  `ALTER FUNCTION "${schemaName}".notifications_function() SET search_path = pg_catalog, pg_temp;`,
                  `ALTER FUNCTION "${schemaName}".workflow_events_function() SET search_path = pg_catalog, pg_temp;`,
                ]
              : []),
          ],
    },
    {
      pg: [
        `CREATE TABLE "${schemaName}"."queues" (
          "queue_id" TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
          "name" TEXT NOT NULL UNIQUE,
          "concurrency" INTEGER,
          "worker_concurrency" INTEGER,
          "rate_limit_max" INTEGER,
          "rate_limit_period_sec" DOUBLE PRECISION,
          "priority_enabled" BOOLEAN NOT NULL DEFAULT FALSE,
          "partition_queue" BOOLEAN NOT NULL DEFAULT FALSE,
          "polling_interval_sec" DOUBLE PRECISION NOT NULL DEFAULT 1.0,
          "created_at" BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000.0)::bigint,
          "updated_at" BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000.0)::bigint
        )`,
      ],
    },
    // Migrations below replace broad indexes on workflow_status with partial
    // indexes targeted at individual query patterns (recovery, troubleshooting,
    // dequeue, rate-limit count). Each index DDL runs CONCURRENTLY on Postgres
    // for safe online deployment; CockroachDB ignores the keyword.
    {
      online: true,
      pg: [`DROP INDEX ${c} IF EXISTS "${schemaName}"."idx_workflow_status_forked_from"`],
    },
    {
      online: true,
      pg: [
        `CREATE INDEX ${c} IF NOT EXISTS "idx_workflow_status_forked_from" ON "${schemaName}"."workflow_status" ("forked_from") WHERE "forked_from" IS NOT NULL`,
      ],
    },
    {
      online: true,
      pg: [`DROP INDEX ${c} IF EXISTS "${schemaName}"."idx_workflow_status_parent_workflow_id"`],
    },
    {
      online: true,
      pg: [
        `CREATE INDEX ${c} IF NOT EXISTS "idx_workflow_status_parent_workflow_id" ON "${schemaName}"."workflow_status" ("parent_workflow_id") WHERE "parent_workflow_id" IS NOT NULL`,
      ],
    },
    {
      online: true,
      pg: [`DROP INDEX ${c} IF EXISTS "${schemaName}"."workflow_status_executor_id_index"`],
    },
    {
      online: true,
      pg: [
        `CREATE UNIQUE INDEX ${c} IF NOT EXISTS "uq_workflow_status_dedup_id" ON "${schemaName}"."workflow_status" ("queue_name", "deduplication_id") WHERE "deduplication_id" IS NOT NULL`,
      ],
    },
    {
      // CockroachDB stores `UNIQUE (...)` constraints as unique indexes, so
      // they must be dropped with DROP INDEX rather than ALTER TABLE DROP CONSTRAINT.
      pg: isCockroach
        ? [`DROP INDEX IF EXISTS "${schemaName}"."uq_workflow_status_queue_name_dedup_id" CASCADE`]
        : [
            `ALTER TABLE "${schemaName}"."workflow_status" DROP CONSTRAINT IF EXISTS "uq_workflow_status_queue_name_dedup_id"`,
          ],
    },
    {
      online: true,
      pg: [
        `CREATE INDEX ${c} IF NOT EXISTS "idx_workflow_status_pending" ON "${schemaName}"."workflow_status" ("created_at") WHERE "status" = 'PENDING'`,
      ],
    },
    {
      online: true,
      pg: [
        `CREATE INDEX ${c} IF NOT EXISTS "idx_workflow_status_failed" ON "${schemaName}"."workflow_status" ("status", "created_at") WHERE "status" IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')`,
      ],
    },
    {
      online: true,
      pg: [`DROP INDEX ${c} IF EXISTS "${schemaName}"."workflow_status_status_index"`],
    },
    {
      online: true,
      pg: [
        `CREATE INDEX ${c} IF NOT EXISTS "idx_workflow_status_in_flight" ON "${schemaName}"."workflow_status" ("queue_name", "status", "priority", "created_at") WHERE "status" IN ('ENQUEUED', 'PENDING')`,
      ],
    },
    {
      pg: [
        `ALTER TABLE "${schemaName}"."workflow_status" ADD COLUMN IF NOT EXISTS "rate_limited" BOOLEAN NOT NULL DEFAULT FALSE`,
      ],
    },
    {
      online: true,
      pg: [
        `CREATE INDEX ${c} IF NOT EXISTS "idx_workflow_status_rate_limited" ON "${schemaName}"."workflow_status" ("queue_name", "started_at_epoch_ms") WHERE "rate_limited" = TRUE`,
      ],
    },
    {
      online: true,
      pg: [`DROP INDEX ${c} IF EXISTS "${schemaName}"."idx_workflow_status_queue_status_started"`],
    },
  ];
}
