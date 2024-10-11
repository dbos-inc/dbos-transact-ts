exports.up = function(knex) {
  return knex.schema.withSchema('dbos')
    .createTable('event_dispatch_kv', function(table) {
        table.text('service_name').notNullable();
        table.text('workflow_fn_name').notNullable();
        table.text('key').notNullable();
        table.text('value');
        table.decimal('update_seq', 38, 0);
        table.decimal('update_time', 38, 15);
        table.primary(['service_name','workflow_fn_name','key']);
    })
};

exports.down = function(knex) {
  return knex.schema.withSchema('dbos')
      .dropTableIfExists('event_dispatch_kv');
};
