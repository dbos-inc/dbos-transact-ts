exports.up = function(knex) {
  return knex.schema.withSchema('dbos')
    .createTable('scheduler_state', function(table) {
        table.text('workflow_fn_name').notNullable();
        table.bigInteger('last_run_time').notNullable();
        table.primary(['workflow_fn_name']);
    })
};

exports.down = function(knex) {
  return knex.schema.withSchema('dbos')
      .dropTableIfExists('scheduler_state');
};
