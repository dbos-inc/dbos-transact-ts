// Due to a naming issue, this migration is mistakenly applied before 20252101000000_workflow_queues_executor_id.
// Please see the 20252101000000_workflow_queues_executor_id migration for more details.

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('operation_outputs', function (table) {
    table.text('child_workflow_id');
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('operation_outputs', function (table) {
    table.dropColumn('child_workflow_id');
  });
};
