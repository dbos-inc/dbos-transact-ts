// Due to a naming issue, this migration is named "20252806000000_streaming.js"
// Please see the 20252101000000_workflow_queues_executor_id migration for more details.

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.withSchema('dbos').createTable('streams', function (table) {
    table.text('workflow_uuid').notNullable();
    table.text('key').notNullable();
    table.text('value').notNullable();
    table.integer('offset').notNullable();
    table.primary(['workflow_uuid', 'key', 'offset']);
    table
      .foreign('workflow_uuid')
      .references('workflow_uuid')
      .inTable('dbos.workflow_status')
      .onUpdate('CASCADE')
      .onDelete('CASCADE');
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.withSchema('dbos').dropTable('streams');
};
