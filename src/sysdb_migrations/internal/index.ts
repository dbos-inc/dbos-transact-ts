/* Auto-generated. */
import type { GeneratedMigration } from '../migration_types';
import { migration as sysdb_1 } from './20240123182943_schema';
import { migration as sysdb_2 } from './20240123182944_dbos_migrations';
import { migration as sysdb_3 } from './20240123183021_tables';
import { migration as sysdb_4 } from './20240123183025_indexes';
import { migration as sysdb_5 } from './20240123183030_triggers';
import { migration as sysdb_6 } from './20240124015239_status_timestamp';
import { migration as sysdb_7 } from './20240201213211_replica_identity';
import { migration as sysdb_8 } from './20240205223925_foreign_keys';
import { migration as sysdb_9 } from './20240207192338_executor_id_index';
import { migration as sysdb_10 } from './20240430090000_tables';
import { migration as sysdb_11 } from './20240516004341_application_version';
import { migration as sysdb_12 } from './20240517000000_status_class_config';
import { migration as sysdb_13 } from './20240621000000_workflow_tries';
import { migration as sysdb_14 } from './20240924000000_workflowqueue';
import { migration as sysdb_15 } from './20241009150000_event_dispatch_kv';
import { migration as sysdb_16 } from './20250312171547_function_name_op_outputs';
import { migration as sysdb_17 } from './20250319190617_add_childid_opoutputs';
import { migration as sysdb_18 } from './20252101000000_workflow_queues_executor_id';
import { migration as sysdb_19 } from './20252501190959_queue_dedup_id';
import { migration as sysdb_20 } from './20252505000000_workflow_timeout';
import { migration as sysdb_21 } from './20252512000000_queue_priority';
import { migration as sysdb_22 } from './20252523000000_consolidate_inputs';
import { migration as sysdb_23 } from './20252528000000_consolidate_queues';
import { migration as sysdb_24 } from './20252806000000_streaming';

export const allMigrations: ReadonlyArray<GeneratedMigration> = [
  sysdb_1,
  sysdb_2,
  sysdb_3,
  sysdb_4,
  sysdb_5,
  sysdb_6,
  sysdb_7,
  sysdb_8,
  sysdb_9,
  sysdb_10,
  sysdb_11,
  sysdb_12,
  sysdb_13,
  sysdb_14,
  sysdb_15,
  sysdb_16,
  sysdb_17,
  sysdb_18,
  sysdb_19,
  sysdb_20,
  sysdb_21,
  sysdb_22,
  sysdb_23,
  sysdb_24,
];
