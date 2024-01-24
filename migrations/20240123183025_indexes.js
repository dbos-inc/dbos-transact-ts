exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
      .table('notifications', function(table) {
        table.index(['destination_uuid', 'topic'], 'idx_workflow_topic');
      });
  };
  
  exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
      .table('notifications', function(table) {
        table.dropIndex(['destination_uuid', 'topic'], 'idx_workflow_topic');
      });
  };
  