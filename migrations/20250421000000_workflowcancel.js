exports.up = function (knex) {
  return knex.schema.withSchema('dbos').createTable('workflow_cancel', function (table) {
    table.text('workflow_id').notNullable();
    table
      .bigInteger('cancelled_at_epoch_ms')
      .notNullable()
      .defaultTo(knex.raw('(EXTRACT(EPOCH FROM now())*1000)::bigint'));
    table.primary(['workflow_id']);
    table.foreign('workflow_id').references('workflow_uuid').inTable('dbos.workflow_status').onDelete('CASCADE');
  });
};

exports.down = function (knex) {
  return knex.schema.withSchema('dbos').dropTableIfExists('workflow_cancel');
};
