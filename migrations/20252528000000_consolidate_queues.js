// Due to a naming issue, this migration is named "20252528000000_consolidate_queues.js" instead of "20250528000000_consolidate_queues.js".
// Please see the 20252101000000_workflow_queues_executor_id migration for more details.

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_status', function (table) {
    table.bigInteger('started_at_epoch_ms').nullable();
    table.text('deduplication_id').nullable();
    table.integer('priority').notNullable().defaultTo(0);
    table.unique(['queue_name', 'deduplication_id'], { indexName: 'uq_workflow_status_queue_name_dedup_id' });
    table.index(['status'], 'workflow_status_status_index');
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_status', function (table) {
    table.dropIndex(['status'], 'workflow_status_status_index');
    table.dropUnique(['queue_name', 'deduplication_id'], 'uq_workflow_status_queue_name_dedup_id');
    table.dropColumn('priority');
    table.dropColumn('deduplication_id');
    table.dropColumn('started_at_epoch_ms');
  });
};
