exports.up = function(knex) {
  return knex.schema.withSchema('dbos')
    .createTable('scheduler_state', function(table) {
        table.text('wf_function').notNullable();
        table.bigInteger('last_wf_sched_time').notNullable();
        table.primary(['wf_function']);
    })
};

exports.down = function(knex) {
  return knex.schema.withSchema('dbos')
      .dropTableIfExists('scheduler_state');
};
