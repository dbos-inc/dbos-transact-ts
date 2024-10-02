exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            table.text('queue_name')
                .defaultTo(null);
        })
        .createTable('workflow_queue', function(table) {
            table.text('queue_name').notNullable();
            table.text('workflow_uuid').notNullable();
            table.bigInteger('created_at_epoch_ms').notNullable().defaultTo(knex.raw('(EXTRACT(EPOCH FROM now())*1000)::bigint'));
            table.bigInteger('started_at_epoch_ms');
            table.bigInteger('completed_at_epoch_ms');
            table.primary(['workflow_uuid']);
            table.foreign('workflow_uuid').references('workflow_uuid').inTable('dbos.workflow_status').onDelete('CASCADE').onUpdate('CASCADE');
        })
  };
  
exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            table.dropColumn('queue_name');
        })
        .dropTableIfExists('workflow_queue');
};
