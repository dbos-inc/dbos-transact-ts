/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function(knex) {
  return knex.schema.withSchema('dbos')
    .alterTable('workflow_status', function(table) {
      table.index('executor_id');
    });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function(knex) {
  return knex.schema.withSchema('dbos')
    .alterTable('workflow_status', function(table) {
      table.dropIndex('executor_id');
    });
};
