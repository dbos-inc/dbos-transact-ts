exports.up = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_queue', function (table) {
    table.text('executor_id');
  });
};

exports.down = function (knex) {
  return knex.schema.withSchema('dbos').table('workflow_queue', function (table) {
    table.dropColumn('executor_id');
  });
};
