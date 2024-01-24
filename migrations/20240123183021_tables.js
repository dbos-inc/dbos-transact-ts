exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
      .createTable('operation_outputs', function(table) {
        table.text('workflow_uuid').notNullable();
        table.integer('function_id').notNullable();
        table.text('output');
        table.text('error');
        table.primary(['workflow_uuid', 'function_id']);
      })
      .createTable('workflow_inputs', function(table) {
        table.text('workflow_uuid').primary().notNullable();
        table.text('inputs').notNullable();
      })
      .createTable('workflow_status', function(table) {
        table.text('workflow_uuid').primary();
        table.text('status');
        table.text('name');
        table.text('authenticated_user');
        table.text('assumed_role');
        table.text('authenticated_roles');
        table.text('request');
        table.text('output');
        table.text('error');
        table.text('executor_id');
      })
      .createTable('notifications', function(table) {
        table.text('destination_uuid').notNullable();
        table.text('topic');
        table.text('message').notNullable();
        table.bigInteger('created_at_epoch_ms').notNullable().defaultTo(knex.raw('(EXTRACT(EPOCH FROM now())*1000)::bigint'));
      })
      .createTable('workflow_events', function(table) {
        table.text('workflow_uuid').notNullable();
        table.text('key').notNullable();
        table.text('value').notNullable();
        table.primary(['workflow_uuid', 'key']);
      });
  };
  
  exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
      .dropTableIfExists('operation_outputs')
      .dropTableIfExists('workflow_inputs')
      .dropTableIfExists('workflow_status')
      .dropTableIfExists('notifications')
      .dropTableIfExists('workflow_events');
  };
  