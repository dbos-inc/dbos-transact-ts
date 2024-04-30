exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
      .createTable('scheduler_state', function(table) {
        table.text('wf_function').notNullable();
        table.bigInteger('last_wf_sched_time').notNullable();
      })
      .createTable('scheduled_wf_running', function(table) {
        table.text('wf_function').notNullable();
        table.bigInteger('scheduled_time').notNullable();
        table.bigInteger('actual_time').notNullable();
      })
  };
  
  exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
      .dropTableIfExists('scheduler_state')
      .dropTableIfExists('scheduled_wf_running');
  };
  