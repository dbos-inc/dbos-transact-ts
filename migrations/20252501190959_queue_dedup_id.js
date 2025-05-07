/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_queue', function (table) {
    table.text('deduplication_id');
    table.unique(['queue_name', 'deduplication_id'], 'workflow_queue_queue_name_deduplication_id_unique');
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_queue', function (table) {
    table.dropUnique(['queue_name', 'deduplication_id'], 'workflow_queue_queue_name_deduplication_id_unique');
    table.dropColumn('deduplication_id');
  });
};
