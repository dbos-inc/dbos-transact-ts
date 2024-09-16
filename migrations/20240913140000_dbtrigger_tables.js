exports.up = function(knex) {
  return knex.schema.withSchema('dbos')
    .createTable('dbtrigger_state', function(table) {
        table.text('workflow_fn_name').notNullable();
        table.bigInteger('last_run_seq');
        table.bigInteger('last_run_time');
        table.primary(['workflow_fn_name']);
    })
};

exports.down = function(knex) {
  return knex.schema.withSchema('dbos')
      .dropTableIfExists('dbtrigger_state');
};
