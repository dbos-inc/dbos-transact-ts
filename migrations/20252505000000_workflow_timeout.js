// Due to a naming issue, this migration is named "20252505000000_workflow_timeout" instead of "20250505000000_workflow_timeout".
// Please see the 20252101000000_workflow_queues_executor_id migration for more details.

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_status', function (table) {
    table.bigInteger('workflow_timeout_ms');
    table.bigInteger('workflow_deadline_epoch_ms');
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_status', function (table) {
    table.dropColumn('workflow_timeout_ms');
    table.dropColumn('workflow_deadline_epoch_ms');
  });
};
