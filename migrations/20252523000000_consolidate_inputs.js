// Due to a naming issue, this migration is named "20252512000000_queue_priority" instead of "20250512000000_queue_priority".
// Please see the 20252101000000_workflow_queues_executor_id migration for more details.

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_status', function (table) {
    table.text('inputs').nullable();
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_status', function (table) {
    table.dropColumn('inputs');
  });
};
