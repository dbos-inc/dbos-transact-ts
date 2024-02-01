exports.up = async function(knex) {
  await knex.raw('create extension if not exists "uuid-ossp"');
  return knex.schema.withSchema('dbos')
    .table('notifications', function(table) {
      table.text('message_uuid').primary().defaultTo(knex.raw('uuid_generate_v4()'));
    })

};

exports.down = function(knex) {
  return knex.schema.withSchema('dbos')
    .table('notifications', function(table) {
      table.dropColumn('message_uuid');
    });
};
