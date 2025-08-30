exports.up = function (knex) {
  return knex.schema.withSchema('dbos').createTable('dbos_migrations', (table) => {
    table.bigInteger('version').notNullable();
    table.primary(['version']);
  });
};

exports.down = function (knex) {
  return knex.schema.withSchema('dbos').dropTable('dbos_migrations');
};
