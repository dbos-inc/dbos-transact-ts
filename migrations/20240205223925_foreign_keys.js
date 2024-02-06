/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function(knex) {
  return knex.schema.withSchema('dbos')
    .alterTable('workflow_status', function(table) {
      table.index('created_at');
    })
    .alterTable('operation_outputs', function(table) {
      table.foreign('workflow_uuid').references('workflow_uuid').inTable('dbos.workflow_status').onDelete('CASCADE').onUpdate('CASCADE');
    })
    .alterTable('workflow_inputs', function(table) {
      table.foreign('workflow_uuid').references('workflow_uuid').inTable('dbos.workflow_status').onDelete('CASCADE').onUpdate('CASCADE');
    })
    .alterTable('notifications', function(table) {
      table.foreign('destination_uuid').references('workflow_uuid').inTable('dbos.workflow_status').onDelete('CASCADE').onUpdate('CASCADE');
    })
    .alterTable('workflow_events', function(table) {
      table.foreign('workflow_uuid').references('workflow_uuid').inTable('dbos.workflow_status').onDelete('CASCADE').onUpdate('CASCADE');
    });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function(knex) {
  return knex.schema.withSchema('dbos')
    .alterTable('workflow_status', function(table) {
      table.dropIndex('created_at');
    })
    .alterTable('operation_outputs', function(table) {
      table.dropForeign('workflow_uuid');
    })
    .alterTable('workflow_inputs', function(table) {
      table.dropForeign('workflow_uuid');
    })
    .alterTable('notifications', function(table) {
      table.dropForeign('destination_uuid');
    })
    .alterTable('workflow_events', function(table) {
      table.dropForeign('workflow_uuid');
    });
};
