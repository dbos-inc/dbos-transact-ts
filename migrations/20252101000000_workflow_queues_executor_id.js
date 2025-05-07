// This migration should have been prefixed with 20250121 instead of 20252101.
// Because of this mistake, correctly prefixed migrations that are added in 2025 will be applied in the wrong order.
// For example, 20250312171547_function_name_op_outputs and 20250319190617_add_childid_opoutputs were added after but get applied before this migration.
// Luckily, those two migrations affect the operation_outputs table, so incorrect ordering is less impactful.

// For the rest of 2025, we need to add 20 to the month field of the prefix so that the migrations are applied in the correct order.
// Example, a migration added on May 1st, 2025 should be prefixed with 20252501.

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_queue', function (table) {
    table.text('executor_id');
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_queue', function (table) {
    table.dropColumn('executor_id');
  });
};
