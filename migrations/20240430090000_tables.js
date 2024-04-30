exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
      .createTable('scheduler_state', function(table) {
        table.text('wf_function').notNullable();
        table.bigInteger('last_wf_sched_time').notNullable();
        table.primary(['wf_function']);
      })
      .createTable('scheduled_wf_running', function(table) {
        table.text('wf_function').notNullable();
        table.bigInteger('scheduled_time').notNullable();
        table.bigInteger('actual_time').notNullable();
        table.primary(['wf_function', 'scheduled_time']);
      })
  };
  
  exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
      .dropTableIfExists('scheduler_state')
      .dropTableIfExists('scheduled_wf_running');
  };
  