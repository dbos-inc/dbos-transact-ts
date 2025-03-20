/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('operation_outputs', function (table) {
    table.text('function_name').notNullable().defaultTo(''); // Function name
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('operation_outputs', function (table) {
    table.dropColumn('function_name');
  });
};
